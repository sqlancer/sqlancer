package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractTableColumn;
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

    public static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.add("An I/O error occurred while sending to the backend");
        errors.add("Conflicts with committed transaction");
        errors.add("cannot be changed");
        errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");

        errors.add("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
        errors.add("but it cannot be referenced from this part of the query");
        errors.add("missing FROM-clause entry for table");

        errors.add("canceling statement due to statement timeout");

        errors.add("non-integer constant in");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY position");
    }

    public static void addCommonTableErrors(ExpectedErrors errors) {
        errors.add("PRIMARY KEY containing column of type 'INET' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'VARBIT' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'INT4RANGE' not yet supported");
        errors.add("INDEX on column of type 'INET' not yet supported");
        errors.add("INDEX on column of type 'VARBIT' not yet supported");
        errors.add("INDEX on column of type 'INT4RANGE' not yet supported");
        errors.add("is not commutative"); // exclude
        errors.add("cannot be changed");
        errors.add("operator requires run-time type coercion"); // exclude
    }

    public static void addCommonExpressionErrors(ExpectedErrors errors) {
        errors.add("syntax error at or near \"(\"");
        errors.add("does not exist");
        errors.add("is not unique");
        errors.add("cannot be changed");
        errors.add("invalid reference to FROM-clause entry for table");

        errors.add("Invalid column number");
        errors.add("specified more than once");
        errors.add("You might need to add explicit type casts");
        errors.add("invalid regular expression");
        errors.add("could not determine which collation to use");
        errors.add("invalid input syntax for integer");
        errors.add("invalid regular expression");
        errors.add("operator does not exist");
        errors.add("quantifier operand invalid");
        errors.add("collation mismatch");
        errors.add("collations are not supported");
        errors.add("operator is not unique");
        errors.add("is not a valid binary digit");
        errors.add("invalid hexadecimal digit");
        errors.add("invalid hexadecimal data: odd number of digits");
        errors.add("zero raised to a negative power is undefined");
        errors.add("cannot convert infinity to numeric");
        errors.add("division by zero");
        errors.add("invalid input syntax for type money");
        errors.add("invalid input syntax for type");
        errors.add("cannot cast type");
        errors.add("value overflows numeric format");
        errors.add("is of type boolean but expression is of type text");
        errors.add("a negative number raised to a non-integer power yields a complex result");
        errors.add("could not determine polymorphic type because input has type unknown");
        addToCharFunctionErrors(errors);
        addBitStringOperationErrors(errors);
        addFunctionErrors(errors);
        addCommonRangeExpressionErrors(errors);
        addCommonRegexExpressionErrors(errors);
    }

    private static void addToCharFunctionErrors(ExpectedErrors errors) {
        errors.add("multiple decimal points");
        errors.add("and decimal point together");
        errors.add("multiple decimal points");
        errors.add("cannot use \"S\" twice");
        errors.add("must be ahead of \"PR\"");
        errors.add("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together");
        errors.add("cannot use \"S\" and \"SG\" together");
        errors.add("cannot use \"S\" and \"MI\" together");
        errors.add("cannot use \"S\" and \"PL\" together");
        errors.add("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together");
        errors.add("is not a number");
    }

    private static void addBitStringOperationErrors(ExpectedErrors errors) {
        errors.add("cannot XOR bit strings of different sizes");
        errors.add("cannot AND bit strings of different sizes");
        errors.add("cannot OR bit strings of different sizes");
        errors.add("must be type boolean, not type text");
    }

    private static void addFunctionErrors(ExpectedErrors errors) {
        errors.add("out of valid range"); // get_bit/get_byte
        errors.add("cannot take logarithm of a negative number");
        errors.add("cannot take logarithm of zero");
        errors.add("requested character too large for encoding"); // chr
        errors.add("null character not permitted"); // chr
        errors.add("requested character not valid for encoding"); // chr
        errors.add("requested length too large"); // repeat
        errors.add("invalid memory alloc request size"); // repeat
        errors.add("encoding conversion from UTF8 to ASCII not supported"); // to_ascii
        errors.add("negative substring length not allowed"); // substr
        errors.add("invalid mask length"); // set_masklen
    }

    private static void addCommonRegexExpressionErrors(ExpectedErrors errors) {
        errors.add("is not a valid hexadecimal digit");
    }

    public static void addCommonRangeExpressionErrors(ExpectedErrors errors) {
        errors.add("range lower bound must be less than or equal to range upper bound");
        errors.add("result of range difference would not be contiguous");
        errors.add("out of range");
        errors.add("malformed range literal");
        errors.add("result of range union would not be contiguous");
    }

    public static void addCommonInsertUpdateErrors(ExpectedErrors errors) {
        errors.add("value too long for type character");
        errors.add("not found in view targetlist");
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
            if (Randomly.getBoolean()) {
                sb.append(" ");
                // disabled https://github.com/yugabyte/yugabyte-db/issues/11357
                // sb.append(" WITH (");
                // ArrayList<StorageParameters> values = new ArrayList<>(Arrays.asList(StorageParameters.values()));
                // errors.add("unrecognized parameter");
                // errors.add("ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables");
                // List<StorageParameters> subset = Randomly.nonEmptySubset(values);
                // int i = 0;
                // for (StorageParameters parameter : subset) {
                // if (i++ != 0) {
                // sb.append(", ");
                // }
                // sb.append(parameter.parameter);
                // sb.append("=");
                // sb.append(parameter.op.apply(globalState.getRandomly()));
                // }
                // sb.append(")");
            } else {
                sb.append(" WITHOUT OIDS ");
            }
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
        YSQLCommon.addCommonExpressionErrors(errors);
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

    public static void addGroupingErrors(ExpectedErrors errors) {
        errors.add("non-integer constant in GROUP BY"); // TODO
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("is not in select list");
        errors.add("aggregate functions are not allowed in GROUP BY");
    }

    public static void addViewErrors(ExpectedErrors errors) {
        errors.add("already exists");
        errors.add("cannot drop columns from view");
        errors.add("non-integer constant in ORDER BY"); // TODO
        errors.add("for SELECT DISTINCT, ORDER BY expressions must appear in select list"); // TODO
        errors.add("cannot change data type of view column");
        errors.add("specified more than once"); // TODO
        errors.add("materialized views must not use temporary tables or views");
        errors.add("does not have the form non-recursive-term UNION [ALL] recursive-term");
        errors.add("is not a view");
        errors.add("non-integer constant in DISTINCT ON");
        errors.add("SELECT DISTINCT ON expressions must match initial ORDER BY expressions");
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
