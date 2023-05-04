package sqlancer.materialize.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeVisitor;

public class MaterializeTableGenerator {

    private final String tableName;
    private final StringBuilder sb = new StringBuilder();
    @SuppressWarnings("unused")
    private boolean isTemporaryTable;
    @SuppressWarnings("unused")
    private final MaterializeSchema newSchema;
    private final List<MaterializeColumn> columnsToBeAdded = new ArrayList<>();
    protected final ExpectedErrors errors = new ExpectedErrors();
    private final MaterializeTable table;
    private final boolean generateOnlyKnown;
    private final MaterializeGlobalState globalState;

    public MaterializeTableGenerator(String tableName, MaterializeSchema newSchema, boolean generateOnlyKnown,
            MaterializeGlobalState globalState) {
        this.tableName = tableName;
        this.newSchema = newSchema;
        this.generateOnlyKnown = generateOnlyKnown;
        this.globalState = globalState;
        table = new MaterializeTable(tableName, columnsToBeAdded, null, null, null, false, false);
        errors.add("invalid input syntax for");
        errors.add("is not unique");
        errors.add("integer out of range");
        errors.add("division by zero");
        errors.add("cannot create partitioned table as inheritance child");
        errors.add("does not support casting");
        errors.add("ERROR: functions in index expression must be marked IMMUTABLE");
        errors.add("functions in partition key expression must be marked IMMUTABLE");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("has no default operator class for access method");
        errors.add("does not exist for access method");
        errors.add("does not accept data type");
        errors.add("but default expression is of type text");
        errors.add("has pseudo-type unknown");
        errors.add("no collation was derived for partition key column");
        errors.add("inherits from generated column but specifies identity");
        errors.add("inherits from generated column but specifies default");
        MaterializeCommon.addCommonExpressionErrors(errors);
        MaterializeCommon.addCommonTableErrors(errors);
    }

    public static SQLQueryAdapter generate(String tableName, MaterializeSchema newSchema, boolean generateOnlyKnown,
            MaterializeGlobalState globalState) {
        return new MaterializeTableGenerator(tableName, newSchema, generateOnlyKnown, globalState).generate();
    }

    protected SQLQueryAdapter generate() {
        sb.append("CREATE");
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
        sb.append(")");
    }

    private void createColumn(String name) throws AssertionError {
        sb.append(name);
        sb.append(" ");
        MaterializeDataType type = MaterializeDataType.getRandomType();
        MaterializeCommon.appendDataType(type, sb, true, generateOnlyKnown, globalState.getCollates());
        MaterializeColumn c = new MaterializeColumn(name, type);
        c.setTable(table);
        columnsToBeAdded.add(c);
        sb.append(" ");
        if (Randomly.getBoolean()) {
            createColumnConstraint(type);
        }
    }

    private enum ColumnConstraint {
        DEFAULT
    };

    private void createColumnConstraint(MaterializeDataType type) {
        List<ColumnConstraint> constraintSubset = Randomly.nonEmptySubset(ColumnConstraint.values());
        for (ColumnConstraint c : constraintSubset) {
            sb.append(" ");
            switch (c) {
            case DEFAULT:
                sb.append("DEFAULT");
                sb.append(" (");
                sb.append(MaterializeVisitor
                        .asString(MaterializeExpressionGenerator.generateExpression(globalState, type)));
                sb.append(")");
                // CREATE TEMPORARY TABLE t1(c0 smallint DEFAULT ('566963878'));
                errors.add("out of range");
                errors.add("is a generated column");
                break;
            default:
                throw new AssertionError(sb);
            }
        }
    }

}
