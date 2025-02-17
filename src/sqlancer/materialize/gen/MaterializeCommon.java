package sqlancer.materialize.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeProvider;
import sqlancer.materialize.MaterializeSchema;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeVisitor;

public final class MaterializeCommon {

    private MaterializeCommon() {
    }

    public static List<String> getCommonFetchErrors() {
        List<String> errors = DBMSCommon.getCommonFetchErrors();

        errors.add("result exceeds max size of");
        errors.add("does not exist");
        errors.add("aggregate functions are not allowed in");
        errors.add("is only defined for finite arguments");

        return errors;
    }

    public static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.addAll(getCommonFetchErrors());
    }

    public static List<String> getCommonTableErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("is not commutative"); // exclude
        errors.add("operator requires run-time type coercion"); // exclude

        return errors;
    }

    public static void addCommonTableErrors(ExpectedErrors errors) {
        errors.addAll(getCommonTableErrors());
    }

    public static List<String> getCommonExpressionErrors() {
        List<String> errors = DBMSCommon.getCommonExpressionErrors();

        errors.add("cannot convert infinity to numeric");
        errors.add("numeric field overflow");
        errors.add("unterminated escape sequence");
        errors.add("cannot be matched");
        errors.add("clause must have type"); // "not" in having doesn't work
        errors.add("argument must have type"); // "not" in having doesn't work
        errors.add("CAST does not support casting from");
        errors.add("aggregate functions are not allowed in");
        errors.add("only defined for finite arguments");
        errors.add("unable to parse column reference in GROUP BY clause"); // TODO

        errors.addAll(getFunctionErrors());
        return errors;
    }

    public static void addCommonExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getCommonExpressionErrors());
    }

    private static List<String> getFunctionErrors() {
        List<String> errors = DBMSCommon.getFunctionErrors();
        errors.add("encoding conversion from UTF8 to ASCII not supported"); // to_ascii
        return errors;
    }

    public static void addCommonRangeExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getCommonExpressionErrors());
    }

    public static List<String> getCommonInsertUpdateErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("value too long for type character");
        errors.add("not found in view targetlist");
        errors.add("CAST does not support casting from");

        return errors;
    }

    public static void addCommonInsertUpdateErrors(ExpectedErrors errors) {
        errors.addAll(getCommonExpressionErrors());
    }

    public static List<String> getGroupingErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("non-integer constant in GROUP BY"); // TODO
        errors.add("unable to parse column reference in GROUP BY clause"); // TODO
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("is not in select list");
        errors.add("aggregate functions are not allowed in");

        return errors;
    }

    public static void addGroupingErrors(ExpectedErrors errors) {
        errors.addAll(getGroupingErrors());
    }

    public static boolean appendDataType(MaterializeDataType type, StringBuilder sb, boolean allowSerial,
            boolean generateOnlyKnown, List<String> opClasses) throws AssertionError {
        boolean serial = false;
        switch (type) {
            case BOOLEAN:
                sb.append("boolean");
                break;
            case INT:
                sb.append(Randomly.fromOptions("smallint", "integer", "bigint"));
                break;
            case TEXT:
                if (Randomly.getBoolean()) {
                    sb.append("TEXT");
                } else {
                    if (MaterializeProvider.generateOnlyKnown || Randomly.getBoolean()) {
                        sb.append("VAR");
                    }
                    sb.append("CHAR");
                    sb.append("(");
                    sb.append(ThreadLocalRandom.current().nextInt(1, 500));
                    sb.append(")");
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
            case BIT:
                sb.append("INT");
                break;
            default:
                throw new AssertionError(type);
        }
        return serial;
    }

    public enum TableConstraints {
        CHECK, PRIMARY_KEY, FOREIGN_KEY, EXCLUDE
    }

    public static void addTableConstraints(boolean excludePrimaryKey, StringBuilder sb, MaterializeTable table,
            MaterializeGlobalState globalState, ExpectedErrors errors) {
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

    public static void addTableConstraint(StringBuilder sb, MaterializeTable table, MaterializeGlobalState globalState,
            ExpectedErrors errors) {
        addTableConstraint(sb, table, globalState, Randomly.fromOptions(TableConstraints.values()), errors);
    }

    private static void addTableConstraint(StringBuilder sb, MaterializeTable table, MaterializeGlobalState globalState,
            TableConstraints t, ExpectedErrors errors) {
        List<MaterializeColumn> randomNonEmptyColumnSubset = table.getRandomNonEmptyColumnSubset();
        List<MaterializeColumn> otherColumns;
        MaterializeCommon.addCommonExpressionErrors(errors);
        switch (t) {
            case CHECK:
                sb.append("CHECK(");
                sb.append(MaterializeVisitor.getExpressionAsString(globalState, MaterializeDataType.BOOLEAN,
                        table.getColumns()));
                sb.append(")");
                errors.add("constraint must be added to child tables too");
                errors.add("missing FROM-clause entry for table");
                break;
            case PRIMARY_KEY:
                sb.append("PRIMARY KEY(");
                sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
                sb.append(")");
                break;
            case FOREIGN_KEY:
                sb.append("FOREIGN KEY (");
                sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
                sb.append(") REFERENCES ");
                MaterializeTable randomOtherTable = globalState.getSchema().getRandomTable(tab -> !tab.isView());
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
                    errors.add(
                            "ERROR: invalid ON DELETE action for foreign key constraint containing generated column");
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
                    sb.append(MaterializeVisitor.asString(MaterializeExpressionGenerator.generateExpression(globalState,
                            table.getColumns(), MaterializeDataType.BOOLEAN)));
                    sb.append(")");
                }
                break;
            default:
                throw new AssertionError(t);
        }
    }

    private static void appendOperator(StringBuilder sb, List<String> operators) {
        sb.append(Randomly.fromList(operators));
    }

    // complete
    private static void appendExcludeElement(StringBuilder sb, MaterializeGlobalState globalState,
            List<MaterializeColumn> columns) {
        if (Randomly.getBoolean()) {
            // append column name
            sb.append(Randomly.fromList(columns).getName());
        } else {
            // append expression
            sb.append("(");
            sb.append(MaterializeVisitor
                    .asString(MaterializeExpressionGenerator.generateExpression(globalState, columns)));
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

    public static String getFreeIndexName(MaterializeSchema s) {
        List<String> indexNames = s.getIndexNames();
        String candidateName;
        do {
            candidateName = DBMSCommon.createIndexName((int) Randomly.getNotCachedInteger(0, 100));
        } while (indexNames.contains(candidateName));
        return candidateName;
    }
}
