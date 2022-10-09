package sqlancer.yugabyte.ysql.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;

public class YSQLTableGenerator {

    protected final ExpectedErrors errors = new ExpectedErrors();
    private final String tableName;
    private final StringBuilder sb = new StringBuilder();
    private final List<YSQLColumn> columnsToBeAdded = new ArrayList<>();
    private final YSQLTable table;
    private final boolean generateOnlyKnown;
    private final YSQLGlobalState globalState;
    private boolean columnCanHavePrimaryKey;
    private boolean columnHasPrimaryKey;
    private boolean isTemporaryTable;

    public YSQLTableGenerator(String tableName, boolean generateOnlyKnown, YSQLGlobalState globalState) {
        this.tableName = tableName;
        this.generateOnlyKnown = generateOnlyKnown;
        this.globalState = globalState;
        table = new YSQLTable(tableName, columnsToBeAdded, null, null, null, false, false);
        // YB catalog specific messages
        errors.add("The catalog snapshot used for this transaction has been invalidated");

        errors.add("PRIMARY KEY containing column of type");
        errors.add("specified value cannot be cast to type boolean for column");
        errors.add("already exists");
        errors.add("invalid input syntax for");
        errors.add("is not unique");
        errors.add("integer out of range");
        errors.add("division by zero");
        errors.add("cannot create partitioned table as inheritance child");
        errors.add("cannot cast");
        errors.add("ERROR: functions in index expression must be marked IMMUTABLE");
        errors.add("functions in partition key expression must be marked IMMUTABLE");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("has no default operator class for access method");
        errors.add("does not exist for access method");
        errors.add("does not accept data type");
        errors.add("but default expression is of type text");
        errors.add("has pseudo-type unknown");
        errors.add("no collation was derived for partition key column");
        errors.add("cannot set colocated true on a non-colocated database");
        errors.add("Cannot split table that does not have primary key");
        errors.add("inherits from generated column but specifies identity");
        errors.add("inherits from generated column but specifies default");
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonTableErrors(errors);
    }

    public static SQLQueryAdapter generate(String tableName, boolean generateOnlyKnown, YSQLGlobalState globalState) {
        return new YSQLTableGenerator(tableName, generateOnlyKnown, globalState).generate();
    }

    private SQLQueryAdapter generate() {
        columnCanHavePrimaryKey = true;
        sb.append("CREATE");
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append(" ");
            isTemporaryTable = true;
            sb.append(Randomly.fromOptions("TEMPORARY", "TEMP"));
        }
        sb.append(" TABLE");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ");
        sb.append(tableName);
        createStandard();
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void createStandard() throws AssertionError {
        sb.append("(");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String name = DBMSCommon.createColumnName(i);
            createColumn(name);
        }
        if (Randomly.getBoolean()) {
            errors.add("constraints on temporary tables may reference only temporary tables");
            errors.add("constraints on unlogged tables may reference only permanent or unlogged tables");
            errors.add("constraints on permanent tables may reference only permanent tables");
            errors.add("cannot be implemented");
            errors.add("there is no unique constraint matching given keys for referenced table");
            errors.add("cannot reference partitioned table");
            errors.add("unsupported ON COMMIT and foreign key combination");
            errors.add("ERROR: invalid ON DELETE action for foreign key constraint containing generated column");
            errors.add("exclusion constraints are not supported on partitioned tables");
            errors.add("option is not yet supported for hash partitioned tables");
            YSQLCommon.addTableConstraints(columnHasPrimaryKey, sb, table, globalState, errors);
        }
        sb.append(")");
        generatePartitionBy();
        YSQLCommon.generateWith(sb, globalState, errors, columnsToBeAdded, isTemporaryTable);
        if (Randomly.getBoolean() && isTemporaryTable) {
            sb.append(" ON COMMIT ");
            // todo ON COMMIT DROP fails and it's known issue
            // sb.append(Randomly.fromOptions("PRESERVE ROWS", "DELETE ROWS", "DROP"));
            sb.append(Randomly.fromOptions("PRESERVE ROWS", "DELETE ROWS"));
            sb.append(" ");
        }
    }

    private void createColumn(String name) throws AssertionError {
        sb.append(name);
        sb.append(" ");
        YSQLDataType type = YSQLDataType.getRandomType();
        boolean serial = YSQLCommon.appendDataType(type, sb, true, generateOnlyKnown, globalState.getCollates());
        YSQLColumn c = new YSQLColumn(name, type);
        c.setTable(table);
        columnsToBeAdded.add(c);
        sb.append(" ");
        if (Randomly.getBoolean()) {
            createColumnConstraint(type, serial);
        }
    }

    private void generatePartitionBy() {
        if (Randomly.getBoolean()) {
            return;
        }
        sb.append(" PARTITION BY ");
        // TODO "RANGE",
        String partitionOption = Randomly.fromOptions("RANGE", "LIST", "HASH");
        sb.append(partitionOption);
        sb.append("(");
        errors.add("unrecognized parameter");
        errors.add("cannot use constant expression");
        errors.add("unrecognized parameter");
        errors.add("unsupported PRIMARY KEY constraint with partition key definition");
        errors.add("which is part of the partition key.");
        errors.add("unsupported UNIQUE constraint with partition key definition");
        errors.add("does not accept data type");
        int n = partitionOption.contentEquals("LIST") ? 1 : Randomly.smallNumber() + 1;
        YSQLErrors.addCommonExpressionErrors(errors);
        for (int i = 0; i < n; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("(");
            YSQLExpression expr = YSQLExpressionGenerator.generateExpression(globalState, columnsToBeAdded);
            sb.append(YSQLVisitor.asString(expr));
            sb.append(")");
            if (Randomly.getBoolean()) {
                sb.append(globalState.getRandomOpclass());
                errors.add("does not exist for access method");
            }
        }
        sb.append(")");
    }

    private void createColumnConstraint(YSQLDataType type, boolean serial) {
        List<ColumnConstraint> constraintSubset = Randomly.nonEmptySubset(ColumnConstraint.values());
        if (Randomly.getBoolean()) {
            // make checks constraints less likely
            constraintSubset.remove(ColumnConstraint.CHECK);
        }
        if (!columnCanHavePrimaryKey || columnHasPrimaryKey) {
            constraintSubset.remove(ColumnConstraint.PRIMARY_KEY);
        }
        if (constraintSubset.contains(ColumnConstraint.GENERATED)
                && constraintSubset.contains(ColumnConstraint.DEFAULT)) {
            // otherwise: ERROR: both default and identity specified for column
            constraintSubset.remove(Randomly.fromOptions(ColumnConstraint.GENERATED, ColumnConstraint.DEFAULT));
        }
        if (constraintSubset.contains(ColumnConstraint.GENERATED) && type != YSQLDataType.INT) {
            // otherwise: ERROR: identity column type must be smallint, integer, or bigint
            constraintSubset.remove(ColumnConstraint.GENERATED);
        }
        if (serial) {
            constraintSubset.remove(ColumnConstraint.GENERATED);
            constraintSubset.remove(ColumnConstraint.DEFAULT);
            constraintSubset.remove(ColumnConstraint.NULL_OR_NOT_NULL);

        }
        for (ColumnConstraint c : constraintSubset) {
            sb.append(" ");
            switch (c) {
            case NULL_OR_NOT_NULL:
                sb.append(Randomly.fromOptions("NOT NULL", "NULL"));
                errors.add("conflicting NULL/NOT NULL declarations");
                break;
            case UNIQUE:
                sb.append("UNIQUE");
                break;
            case PRIMARY_KEY:
                sb.append("PRIMARY KEY");
                columnHasPrimaryKey = true;
                break;
            case DEFAULT:
                sb.append("DEFAULT");
                sb.append(" (");
                sb.append(YSQLVisitor.asString(YSQLExpressionGenerator.generateExpression(globalState, type)));
                sb.append(")");
                errors.add("out of range");
                errors.add("is a generated column");
                break;
            case CHECK:
                sb.append("CHECK (");
                sb.append(YSQLVisitor.asString(YSQLExpressionGenerator.generateExpression(globalState, columnsToBeAdded,
                        YSQLDataType.BOOLEAN)));
                sb.append(")");
                errors.add("out of range");
                break;
            case GENERATED:
                sb.append("GENERATED ");
                if (Randomly.getBoolean()) {
                    sb.append(" ALWAYS AS (");
                    sb.append(YSQLVisitor
                            .asString(YSQLExpressionGenerator.generateExpression(globalState, columnsToBeAdded, type)));
                    sb.append(") STORED");
                    errors.add("A generated column cannot reference another generated column.");
                    errors.add("cannot use generated column in partition key");
                    errors.add("generation expression is not immutable");
                    errors.add("cannot use column reference in DEFAULT expression");
                } else {
                    sb.append(Randomly.fromOptions("ALWAYS", "BY DEFAULT"));
                    sb.append(" AS IDENTITY");
                }
                break;
            default:
                throw new AssertionError(sb);
            }
        }
    }

    private enum ColumnConstraint {
        NULL_OR_NOT_NULL, UNIQUE, PRIMARY_KEY, DEFAULT, CHECK, GENERATED
    }

}
