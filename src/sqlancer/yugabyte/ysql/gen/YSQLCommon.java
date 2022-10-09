package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLProvider;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;

public final class YSQLCommon {

    private YSQLCommon() {
    }

    public static boolean appendDataType(YSQLDataType type, StringBuilder sb, boolean allowSerial,
            boolean generateOnlyKnown, List<String> opClasses) throws AssertionError {
        boolean serial = false;
        switch (type) {
        case BOOLEAN:
            sb.append("boolean");
            break;
        case INT:
            if (Randomly.getBoolean() && allowSerial) {
                serial = true;
                sb.append(Randomly.fromOptions("serial", "bigserial"));
            } else {
                sb.append(Randomly.fromOptions("smallint", "integer", "bigint"));
            }
            break;
        case TEXT:
            if (Randomly.getBoolean()) {
                sb.append("TEXT");
            } else if (Randomly.getBoolean()) {
                // TODO: support CHAR (without VAR)
                if (YSQLProvider.generateOnlyKnown || Randomly.getBoolean()) {
                    sb.append("VAR");
                }
                sb.append("CHAR");
                sb.append("(");
                sb.append(ThreadLocalRandom.current().nextInt(1, 500));
                sb.append(")");
            } else {
                sb.append("name");
            }
            break;
        case DECIMAL:
            sb.append("DECIMAL");
            break;
        case FLOAT:
        case REAL:
            if (Randomly.getBoolean()) {
                sb.append("REAL");
            } else {
                sb.append("FLOAT");
            }
            break;
        case RANGE:
            sb.append(Randomly.fromOptions("int4range", "int4range")); // , "int8range", "numrange"
            break;
        case MONEY:
            sb.append("money");
            break;
        case BYTEA:
            sb.append("bytea");
            break;
        case BIT:
            sb.append("BIT");
            // if (Randomly.getBoolean()) {
            sb.append(" VARYING");
            // }
            sb.append("(");
            sb.append(Randomly.getNotCachedInteger(1, 500));
            sb.append(")");
            break;
        case INET:
            sb.append("inet");
            break;
        default:
            throw new AssertionError(type);
        }
        return serial;
    }

    public static void generateWith(StringBuilder sb, YSQLGlobalState globalState, ExpectedErrors errors,
            List<YSQLColumn> columnsToBeAdded, boolean isTemporaryTable) {
        if (Randomly.getBoolean()) {
            sb.append(" WITHOUT OIDS ");
        } else if (Randomly.getBoolean() && !isTemporaryTable) {
            if (Randomly.getBoolean()) {
                sb.append(" SPLIT INTO ");
                sb.append(Randomly.smallNumber() + 1);
                sb.append(" TABLETS ");

                errors.add("cannot create colocated table with split option");
                errors.add("columns must be present to split by number of tablets");
                errors.add("option is not yet supported for hash partitioned tables");
            } else {
                sb.append(" SPLIT AT VALUES (");

                errors.add("cannot create colocated table with split option");
                errors.add("SPLIT AT option is not yet supported for hash partitioned tables");
                errors.add("Cannot have duplicate split rows"); // just in case

                boolean hasBoolean = false;
                for (YSQLColumn column : columnsToBeAdded) {
                    if (column.getType().equals(YSQLDataType.BOOLEAN)) {
                        hasBoolean = true;
                        break;
                    }
                }

                int splits = hasBoolean ? 2 : Randomly.smallNumber() + 2;
                long start = Randomly.smallNumber();
                boolean[] bools = { false, true };
                for (int i = 1; i <= splits; i++) {
                    int size = columnsToBeAdded.size();
                    int counter = 1;
                    for (YSQLColumn c : columnsToBeAdded) {
                        sb.append("(");
                        switch (c.getType()) {
                        case INT:
                        case REAL:
                            sb.append(YSQLConstant.createDoubleConstant(start));
                        case FLOAT:
                            sb.append(YSQLConstant.createIntConstant(start));
                            break;
                        case BOOLEAN:
                            sb.append(YSQLConstant.createBooleanConstant(bools[i - 1]));
                            break;
                        case TEXT:
                            sb.append(YSQLConstant.createTextConstant(String.valueOf(start)));
                            break;
                        default:
                            throw new IgnoreMeException();
                        }
                        sb.append(")");
                        counter++;
                        start += Randomly.smallNumber() + 1;
                        if (counter <= size) {
                            sb.append(",");
                        }
                    }

                    if (i < splits) {
                        sb.append(",");
                    }
                }
                sb.append(")");
            }
        } else if (Randomly.getBoolean()) {
            errors.add("Cannot use TABLEGROUP with TEMP table");
            sb.append(" TABLEGROUP tg").append(
                    Randomly.getNotCachedInteger(1, (int) YSQLTableGroupGenerator.UNIQUE_TABLEGROUP_COUNTER.get()));
        }
    }

    public static void addTableConstraints(boolean excludePrimaryKey, StringBuilder sb, YSQLTable table,
            YSQLGlobalState globalState, ExpectedErrors errors) {
        // TODO constraint name
        List<TableConstraints> tableConstraints = Randomly.nonEmptySubset(TableConstraints.values());
        if (excludePrimaryKey) {
            tableConstraints.remove(TableConstraints.PRIMARY_KEY);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            tableConstraints.remove(TableConstraints.FOREIGN_KEY);
        }
        for (TableConstraints t : tableConstraints) {
            sb.append(", ");
            // TODO add index parameters
            addTableConstraint(sb, table, globalState, t, errors);
        }
    }

    public static void addTableConstraint(StringBuilder sb, YSQLTable table, YSQLGlobalState globalState,
            ExpectedErrors errors) {
        addTableConstraint(sb, table, globalState, Randomly.fromOptions(TableConstraints.values()), errors);
    }

    private static void addTableConstraint(StringBuilder sb, YSQLTable table, YSQLGlobalState globalState,
            TableConstraints t, ExpectedErrors errors) {
        List<YSQLColumn> randomNonEmptyColumnSubset = table.getRandomNonEmptyColumnSubset();
        List<YSQLColumn> otherColumns;
        YSQLErrors.addCommonExpressionErrors(errors);
        switch (t) {
        case CHECK:
            sb.append("CHECK(");
            sb.append(YSQLVisitor.getExpressionAsString(globalState, YSQLDataType.BOOLEAN, table.getColumns()));
            sb.append(")");
            errors.add("constraint must be added to child tables too");
            errors.add("missing FROM-clause entry for table");
            break;
        case UNIQUE:
            sb.append("UNIQUE(");
            sb.append(randomNonEmptyColumnSubset.stream().map(AbstractTableColumn::getName)
                    .collect(Collectors.joining(", ")));
            sb.append(")");
            break;
        case PRIMARY_KEY:
            sb.append("PRIMARY KEY(");
            sb.append(randomNonEmptyColumnSubset.stream().map(AbstractTableColumn::getName)
                    .collect(Collectors.joining(", ")));
            sb.append(")");
            break;
        case FOREIGN_KEY:
            sb.append("FOREIGN KEY (");
            sb.append(randomNonEmptyColumnSubset.stream().map(AbstractTableColumn::getName)
                    .collect(Collectors.joining(", ")));
            sb.append(") REFERENCES ");
            YSQLTable randomOtherTable = globalState.getSchema().getRandomTable(tab -> !tab.isView());
            sb.append(randomOtherTable.getName());
            if (randomOtherTable.getColumns().size() < randomNonEmptyColumnSubset.size()) {
                throw new IgnoreMeException();
            }
            otherColumns = randomOtherTable.getRandomNonEmptyColumnSubset(randomNonEmptyColumnSubset.size());
            sb.append("(");
            sb.append(otherColumns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
            sb.append(")");
            if (Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("MATCH FULL", "MATCH SIMPLE"));
            }
            if (Randomly.getBoolean()) {
                sb.append(" ON DELETE ");
                errors.add("ERROR: invalid ON DELETE action for foreign key constraint containing generated column");
                deleteOrUpdateAction(sb);
            }
            if (Randomly.getBoolean()) {
                sb.append(" ON UPDATE ");
                errors.add("invalid ON UPDATE action for foreign key constraint containing generated column");
                deleteOrUpdateAction(sb);
            }
            if (Randomly.getBoolean()) {
                sb.append(" ");
                if (Randomly.getBoolean()) {
                    sb.append("DEFERRABLE");
                    if (Randomly.getBoolean()) {
                        sb.append(" ");
                        sb.append(Randomly.fromOptions("INITIALLY DEFERRED", "INITIALLY IMMEDIATE"));
                    }
                } else {
                    sb.append("NOT DEFERRABLE");
                }
            }
            break;
        default:
            throw new AssertionError(t);
        }
    }

    private static void deleteOrUpdateAction(StringBuilder sb) {
        sb.append(Randomly.fromOptions("NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"));
    }

    public enum TableConstraints {
        CHECK, UNIQUE, PRIMARY_KEY, FOREIGN_KEY
    }

    // private enum StorageParameters {
    // COLOCATED("COLOCATED", (r) -> Randomly.getBoolean());
    // // TODO
    //
    // private final String parameter;
    // private final Function<Randomly, Object> op;
    //
    // StorageParameters(String parameter, Function<Randomly, Object> op) {
    // this.parameter = parameter;
    // this.op = op;
    // }
    // }

}
