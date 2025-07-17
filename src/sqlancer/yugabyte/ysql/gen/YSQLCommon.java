package sqlancer.yugabyte.ysql.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
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
        case SMALLINT:
            sb.append("smallint");
            break;
        case INT:
            if (Randomly.getBoolean() && allowSerial) {
                serial = true;
                sb.append(Randomly.fromOptions("serial", "bigserial"));
            } else {
                sb.append("integer");
            }
            break;
        case BIGINT:
            sb.append("bigint");
            break;
        case VARCHAR:
            sb.append("VARCHAR");
            sb.append("(");
            sb.append(ThreadLocalRandom.current().nextInt(1, 500));
            sb.append(")");
            break;
        case CHAR:
            sb.append("CHAR");
            sb.append("(");
            sb.append(ThreadLocalRandom.current().nextInt(1, 500));
            sb.append(")");
            break;
        case TEXT:
            if (Randomly.getBoolean()) {
                sb.append("TEXT");
            } else {
                sb.append("name"); // PostgreSQL name type
            }
            break;
        case NUMERIC:
            sb.append("NUMERIC");
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(1, 38)); // precision
                if (Randomly.getBoolean()) {
                    sb.append(",");
                    sb.append(Randomly.getNotCachedInteger(0, 30)); // scale
                }
                sb.append(")");
            }
            break;
        case DECIMAL:
            sb.append("DECIMAL");
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(1, 38)); // precision
                if (Randomly.getBoolean()) {
                    sb.append(",");
                    sb.append(Randomly.getNotCachedInteger(0, 30)); // scale
                }
                sb.append(")");
            }
            break;
        case REAL:
            sb.append("REAL");
            break;
        case DOUBLE_PRECISION:
            sb.append("DOUBLE PRECISION");
            break;
        case FLOAT:
            sb.append("FLOAT");
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(1, 53));
                sb.append(")");
            }
            break;
        case DATE:
            sb.append("DATE");
            break;
        case TIME:
            sb.append("TIME");
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(0, 6)); // precision
                sb.append(")");
            }
            break;
        case TIMESTAMP:
            sb.append("TIMESTAMP");
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(0, 6)); // precision
                sb.append(")");
            }
            break;
        case TIMESTAMPTZ:
            sb.append("TIMESTAMPTZ");
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(0, 6)); // precision
                sb.append(")");
            }
            break;
        case INTERVAL:
            sb.append("INTERVAL");
            break;
        case UUID:
            sb.append("UUID");
            break;
        case JSON:
            sb.append("JSON");
            break;
        case JSONB:
            sb.append("JSONB");
            break;
        case INT4RANGE:
            sb.append("int4range");
            break;
        case INT8RANGE:
            sb.append("int8range");
            break;
        case NUMRANGE:
            sb.append("numrange");
            break;
        case TSRANGE:
            sb.append("tsrange");
            break;
        case TSTZRANGE:
            sb.append("tstzrange");
            break;
        case DATERANGE:
            sb.append("daterange");
            break;
        case INT_ARRAY:
            sb.append("integer[]");
            break;
        case TEXT_ARRAY:
            sb.append("text[]");
            break;
        case BOOLEAN_ARRAY:
            sb.append("boolean[]");
            break;
        case RANGE:
            sb.append(Randomly.fromOptions("int4range", "int8range", "numrange"));
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
        case CIDR:
            sb.append("cidr");
            break;
        case MACADDR:
            sb.append("macaddr");
            break;
        case POINT:
            sb.append("point");
            break;
        case LINE:
            sb.append("line");
            break;
        case LSEG:
            sb.append("lseg");
            break;
        case BOX:
            sb.append("box");
            break;
        case PATH:
            sb.append("path");
            break;
        case POLYGON:
            sb.append("polygon");
            break;
        case CIRCLE:
            sb.append("circle");
            break;
        default:
            throw new AssertionError(type);
        }
        return serial;
    }

    public static void generateWith(StringBuilder sb, YSQLGlobalState globalState, ExpectedErrors errors,
            List<YSQLColumn> columnsToBeAdded, boolean isTemporaryTable) {
        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                sb.append(" WITHOUT OIDS ");
            } else {
                sb.append(" WITH (");
                ArrayList<StorageParameters> values = new ArrayList<>(Arrays.asList(StorageParameters.values()));
                errors.add("unrecognized parameter");
                errors.add("ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables");
                errors.add("no collation was derived for partition key column");
                errors.add("true on a non-colocated database");
                List<StorageParameters> subset = Randomly.nonEmptySubset(values);
                int i = 0;
                for (StorageParameters parameter : subset) {
                    if (i++ != 0) {
                        sb.append(", ");
                    }
                    sb.append(parameter.parameter);
                    sb.append("=");
                    sb.append(parameter.op.apply(globalState.getRandomly()));
                }
                sb.append(")");
            }
        } else if (Randomly.getBoolean() && !isTemporaryTable) {
            if (Randomly.getBoolean()) {
                sb.append(" SPLIT INTO ");
                sb.append(Randomly.smallNumber() + 1);
                sb.append(" TABLETS ");

                errors.add("with split option");
                errors.add("columns must be present to split by number of tablets");
                errors.add("option is not yet supported for hash partitioned tables");
            } else {
                sb.append(" SPLIT AT VALUES (");

                errors.add("table with split option");
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
        YSQLErrors.addTransactionErrors(errors);
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

    private enum StorageParameters {
        COLOCATION("COLOCATION", (r) -> Randomly.getBoolean());

        private final String parameter;
        private final Function<Randomly, Object> op;

        StorageParameters(String parameter, Function<Randomly, Object> op) {
            this.parameter = parameter;
            this.op = op;
        }
    }

}
