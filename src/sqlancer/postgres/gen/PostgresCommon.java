package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;

public final class PostgresCommon {

    private PostgresCommon() {
    }

    public static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.add("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
        errors.add("but it cannot be referenced from this part of the query");
        errors.add("missing FROM-clause entry for table");

        errors.add("canceling statement due to statement timeout");

        errors.add("non-integer constant in GROUP BY");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY position");
    }

    public static void addCommonTableErrors(ExpectedErrors errors) {
        errors.add("is not commutative"); // exclude
        errors.add("operator requires run-time type coercion"); // exclude
    }

    public static void addCommonExpressionErrors(ExpectedErrors errors) {
        errors.add("You might need to add explicit type casts");
        errors.add("invalid regular expression");
        errors.add("could not determine which collation to use");
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
        errors.add("LIKE pattern must not end with escape character");
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

    public static boolean appendDataType(PostgresDataType type, StringBuilder sb, boolean allowSerial,
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
                if (PostgresProvider.generateOnlyKnown || Randomly.getBoolean()) {
                    sb.append("VAR");
                }
                sb.append("CHAR");
                sb.append("(");
                sb.append(ThreadLocalRandom.current().nextInt(1, 500));
                sb.append(")");
            } else {
                sb.append("name");
            }
            if (Randomly.getBoolean() && !PostgresProvider.generateOnlyKnown) {
                sb.append(" COLLATE ");
                sb.append('"');
                sb.append(Randomly.fromList(opClasses));
                sb.append('"');
            }
            break;
        case DECIMAL:
            sb.append("DECIMAL");
            break;
        case FLOAT:
            sb.append("REAL");
            break;
        case REAL:
            sb.append("FLOAT");
            break;
        case RANGE:
            sb.append(Randomly.fromOptions("int4range", "int4range")); // , "int8range", "numrange"
            break;
        case MONEY:
            sb.append("money");
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

    public enum TableConstraints {
        CHECK, UNIQUE, PRIMARY_KEY, FOREIGN_KEY, EXCLUDE
    }

    private enum StorageParameters {
        FILLFACTOR("fillfactor", (r) -> r.getInteger(10, 100)),
        // toast_tuple_target
        PARALLEL_WORKERS("parallel_workers", (r) -> r.getInteger(0, 1024)),
        AUTOVACUUM_ENABLED("autovacuum_enabled", (r) -> Randomly.fromOptions(0, 1)),
        AUTOVACUUM_VACUUM_THRESHOLD("autovacuum_vacuum_threshold", (r) -> r.getInteger(0, 2147483647)),
        OIDS("oids", (r) -> Randomly.fromOptions(0, 1)),
        AUTOVACUUM_VACUUM_SCALE_FACTOR("autovacuum_vacuum_scale_factor",
                (r) -> Randomly.fromOptions(0, 0.00001, 0.01, 0.1, 0.2, 0.5, 0.8, 0.9, 1)),
        AUTOVACUUM_ANALYZE_THRESHOLD("autovacuum_analyze_threshold", (r) -> r.getLong(0, Integer.MAX_VALUE)),
        AUTOVACUUM_ANALYZE_SCALE_FACTOR("autovacuum_analyze_scale_factor",
                (r) -> Randomly.fromOptions(0, 0.00001, 0.01, 0.1, 0.2, 0.5, 0.8, 0.9, 1)),
        AUTOVACUUM_VACUUM_COST_DELAY("autovacuum_vacuum_cost_delay", (r) -> r.getLong(0, 100)),
        AUTOVACUUM_VACUUM_COST_LIMIT("autovacuum_vacuum_cost_limit", (r) -> r.getLong(1, 10000)),
        AUTOVACUUM_FREEZE_MIN_AGE("autovacuum_freeze_min_age", (r) -> r.getLong(0, 1000000000)),
        AUTOVACUUM_FREEZE_MAX_AGE("autovacuum_freeze_max_age", (r) -> r.getLong(100000, 2000000000)),
        AUTOVACUUM_FREEZE_TABLE_AGE("autovacuum_freeze_table_age", (r) -> r.getLong(0, 2000000000));
        // TODO

        private String parameter;
        private Function<Randomly, Object> op;

        StorageParameters(String parameter, Function<Randomly, Object> op) {
            this.parameter = parameter;
            this.op = op;
        }
    }

    public static void generateWith(StringBuilder sb, PostgresGlobalState globalState, ExpectedErrors errors) {
        if (Randomly.getBoolean()) {
            sb.append(" WITH (");
            ArrayList<StorageParameters> values = new ArrayList<>(Arrays.asList(StorageParameters.values()));
            values.remove(StorageParameters.OIDS);
            errors.add("unrecognized parameter");
            errors.add("ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables");
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
    }

    public static void addTableConstraints(boolean excludePrimaryKey, StringBuilder sb, PostgresTable table,
            PostgresGlobalState globalState, ExpectedErrors errors) {
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

    public static void addTableConstraint(StringBuilder sb, PostgresTable table, PostgresGlobalState globalState,
            ExpectedErrors errors) {
        addTableConstraint(sb, table, globalState, Randomly.fromOptions(TableConstraints.values()), errors);
    }

    private static void addTableConstraint(StringBuilder sb, PostgresTable table, PostgresGlobalState globalState,
            TableConstraints t, ExpectedErrors errors) {
        List<PostgresColumn> randomNonEmptyColumnSubset = table.getRandomNonEmptyColumnSubset();
        List<PostgresColumn> otherColumns;
        PostgresCommon.addCommonExpressionErrors(errors);
        switch (t) {
        case CHECK:
            sb.append("CHECK(");
            sb.append(PostgresVisitor.getExpressionAsString(globalState, PostgresDataType.BOOLEAN, table.getColumns()));
            sb.append(")");
            errors.add("constraint must be added to child tables too");
            errors.add("missing FROM-clause entry for table");
            break;
        case UNIQUE:
            sb.append("UNIQUE(");
            sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
            appendIndexParameters(sb, globalState, errors);
            break;
        case PRIMARY_KEY:
            sb.append("PRIMARY KEY(");
            sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
            appendIndexParameters(sb, globalState, errors);
            break;
        case FOREIGN_KEY:
            sb.append("FOREIGN KEY (");
            sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(") REFERENCES ");
            PostgresTable randomOtherTable = globalState.getSchema().getRandomTable(tab -> !tab.isView());
            sb.append(randomOtherTable.getName());
            if (randomOtherTable.getColumns().size() < randomNonEmptyColumnSubset.size()) {
                throw new IgnoreMeException();
            }
            otherColumns = randomOtherTable.getRandomNonEmptyColumnSubset(randomNonEmptyColumnSubset.size());
            sb.append("(");
            sb.append(otherColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
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
        case EXCLUDE:
            sb.append("EXCLUDE ");
            sb.append("(");
            // TODO [USING index_method ]
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                appendExcludeElement(sb, globalState, table.getColumns());
                sb.append(" WITH ");
                appendOperator(sb, globalState.getOperators());
            }
            sb.append(")");
            appendIndexParameters(sb, globalState, errors);
            errors.add("is not valid");
            errors.add("no operator matches");
            errors.add("operator does not exist");
            errors.add("unknown has no default operator class");
            errors.add("exclusion constraints are not supported on partitioned tables");
            errors.add("The exclusion operator must be related to the index operator class for the constraint");
            errors.add("could not create exclusion constraint");
            // TODO: index parameters
            if (Randomly.getBoolean()) {
                sb.append(" WHERE ");
                sb.append("(");
                sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(globalState,
                        table.getColumns(), PostgresDataType.BOOLEAN)));
                sb.append(")");
            }
            break;
        default:
            throw new AssertionError(t);
        }
    }

    private static void appendIndexParameters(StringBuilder sb, PostgresGlobalState globalState,
            ExpectedErrors errors) {
        if (Randomly.getBoolean()) {
            generateWith(sb, globalState, errors);
        }
        // TODO: [ USING INDEX TABLESPACE tablespace ]
    }

    private static void appendOperator(StringBuilder sb, List<String> operators) {
        sb.append(Randomly.fromList(operators));
    }

    // complete
    private static void appendExcludeElement(StringBuilder sb, PostgresGlobalState globalState,
            List<PostgresColumn> columns) {
        if (Randomly.getBoolean()) {
            // append column name
            sb.append(Randomly.fromList(columns).getName());
        } else {
            // append expression
            sb.append("(");
            sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(globalState, columns)));
            sb.append(")");
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromList(globalState.getOpClasses()));
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("ASC", "DESC"));
        }
        if (Randomly.getBoolean()) {
            sb.append(" NULLS ");
            sb.append(Randomly.fromOptions("FIRST", "LAST"));
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

}
