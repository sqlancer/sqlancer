package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBBugs;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBCompositeDataType;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBTableGenerator {

    private boolean allowPrimaryKey;
    private final List<TiDBColumn> columns = new ArrayList<>();
    private boolean primaryKeyAsTableConstraints;
    private final ExpectedErrors errors = new ExpectedErrors();

    public SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        errors.add("Information schema is changed during the execution of the statement");
        String tableName = globalState.getSchema().getFreeTableName();
        int nrColumns = Randomly.smallNumber() + 1;
        allowPrimaryKey = Randomly.getBoolean();
        primaryKeyAsTableConstraints = allowPrimaryKey && Randomly.getBoolean();
        for (int i = 0; i < nrColumns; i++) {
            TiDBColumn fakeColumn = new TiDBColumn("c" + i, null, false, false);
            columns.add(fakeColumn);
        }
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(columns);

        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(tableName);

        if (Randomly.getBoolean() && globalState.getSchema().getDatabaseTables().size() > 0) {
            sb.append(" LIKE ");
            TiDBTable otherTable = globalState.getSchema().getRandomTable();
            sb.append(otherTable.getName());
        } else {
            createNewTable(gen, sb);
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void createNewTable(TiDBExpressionGenerator gen, StringBuilder sb) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            TiDBCompositeDataType type;
            do {
                type = TiDBCompositeDataType.getRandom();
            } while (type.getPrimitiveDataType() == TiDBDataType.INT && type.getSize() < 4
                    || type.getPrimitiveDataType() == TiDBDataType.BOOL); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/49
            appendType(sb, type);
            sb.append(" ");
            boolean isGeneratedColumn = Randomly.getBooleanWithRatherLowProbability();
            if (isGeneratedColumn) {
                sb.append(" AS (");
                sb.append(TiDBVisitor.asString(gen.generateExpression()));
                sb.append(") ");
                sb.append(Randomly.fromOptions("STORED", "VIRTUAL"));
                sb.append(" ");
                errors.add("You have an error in your SQL syntax"); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/53
                errors.add("Generated column can refer only to generated columns defined prior to it");
                errors.add(
                        "'Defining a virtual generated column as primary key' is not supported for generated columns.");
                errors.add("contains a disallowed function.");
                errors.add("cannot refer to auto-increment column");
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append("CHECK (");
                sb.append(TiDBVisitor.asString(gen.generateExpression()));
                sb.append(") ");
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append("NOT NULL ");
            }
            if (Randomly.getBoolean() && type.getPrimitiveDataType() != TiDBDataType.TEXT
                    && type.getPrimitiveDataType() != TiDBDataType.BLOB && !isGeneratedColumn) {
                sb.append("DEFAULT ");
                sb.append(TiDBVisitor.asString(gen.generateConstant()));
                sb.append(" ");
                errors.add("Invalid default value");
                errors.add(
                        "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead");
            }
            if (type.getPrimitiveDataType() == TiDBDataType.INT && Randomly.getBooleanWithRatherLowProbability()
                    && !isGeneratedColumn) {
                sb.append(" AUTO_INCREMENT ");
                errors.add("there can be only one auto column and it must be defined as a key");
            }
            if (Randomly.getBooleanWithRatherLowProbability() && canUseAsUnique(type)) {
                sb.append("UNIQUE ");
            }
            if (Randomly.getBooleanWithRatherLowProbability() && allowPrimaryKey && !primaryKeyAsTableConstraints
                    && canUseAsUnique(type) && !isGeneratedColumn) {
                sb.append("PRIMARY KEY ");
                allowPrimaryKey = false;
            }
        }
        if (primaryKeyAsTableConstraints) {
            sb.append(", PRIMARY KEY(");
            sb.append(
                    Randomly.nonEmptySubset(columns).stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
            // TODO: do nto include blob/text columns here
            errors.add(" used in key specification without a key length");
        }
        sb.append(")");
        if (Randomly.getBooleanWithRatherLowProbability()
                && !TiDBBugs.bug14 /* there are also a number of unresolved other partitioning bugs */) {
            sb.append("PARTITION BY HASH(");
            sb.append(TiDBVisitor.asString(gen.generateExpression()));
            sb.append(") ");
            sb.append("PARTITIONS ");
            sb.append(Randomly.getNotCachedInteger(1, 100));
            errors.add(
                    "Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed");
            errors.add("This partition function is not allowed");
            errors.add("A PRIMARY KEY must include all columns in the table's partitioning function");
            errors.add("A UNIQUE INDEX must include all columns in the table's partitioning function");
            errors.add("is of a not allowed type for this type of partitioning");
            errors.add("The PARTITION function returns the wrong type");
            if (TiDBBugs.bug16) {
                errors.add("UnknownType: *ast.WhenClause");
            }
        }
        List<Action> actions = Randomly.nonEmptySubset(Action.values());
        for (Action a : actions) {
            sb.append(" ");
            switch (a) {
            case AUTO_INCREMENT:
                sb.append("AUTO_INCREMENT=");
                sb.append(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE));
                break;
            case PRE_SPLIT_REGIONS:
                sb.append("PRE_SPLIT_REGIONS=");
                sb.append(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE));
                break;
            case SHARD_ROW_ID_BITS:
                sb.append("SHARD_ROW_ID_BITS=");
                sb.append(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE));
                errors.add("Unsupported shard_row_id_bits for table with primary key as row id");
                break;
            default:
                throw new AssertionError(a);
            }
        }
    }

    private boolean canUseAsUnique(TiDBCompositeDataType type) {
        return type.getPrimitiveDataType() != TiDBDataType.TEXT && type.getPrimitiveDataType() != TiDBDataType.BLOB;
    }

    private void appendType(StringBuilder sb, TiDBCompositeDataType type) {
        if (type.getPrimitiveDataType() == TiDBDataType.CHAR) {
            throw new IgnoreMeException();
        }
        sb.append(type.toString());
        appendSpecifiers(sb, type.getPrimitiveDataType());
        appendSizeSpecifiers(sb, type.getPrimitiveDataType());
    }

    private enum Action {
        AUTO_INCREMENT, PRE_SPLIT_REGIONS, SHARD_ROW_ID_BITS
    }

    private void appendSizeSpecifiers(StringBuilder sb, TiDBDataType type) {
        if (type.isNumeric() && Randomly.getBoolean() && !TiDBBugs.bug16028) {
            sb.append(" UNSIGNED");
        }
        if (type.isNumeric() && Randomly.getBoolean()
                && !TiDBBugs.bug16028 /* seems to be the same bug as https://github.com/pingcap/tidb/issues/16028 */) {
            sb.append(" ZEROFILL");
        }
    }

    static void appendSpecifiers(StringBuilder sb, TiDBDataType type) {
        if (type == TiDBDataType.TEXT || type == TiDBDataType.BLOB) {
            sb.append("(");
            sb.append(Randomly.getNotCachedInteger(1, 500));
            sb.append(")");
        }
    }
}
