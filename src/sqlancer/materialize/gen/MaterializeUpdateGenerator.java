package sqlancer.materialize.gen;

import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeVisitor;
import sqlancer.materialize.ast.MaterializeExpression;

public final class MaterializeUpdateGenerator extends AbstractUpdateGenerator<MaterializeColumn> {

    private final MaterializeGlobalState globalState;
    private MaterializeTable randomTable;

    private MaterializeUpdateGenerator(MaterializeGlobalState globalState) {
        this.globalState = globalState;
        errors.addAll(Arrays.asList("conflicting key value violates exclusion constraint",
                "reached maximum value of sequence", "violates foreign key constraint", "violates not-null constraint",
                "violates unique constraint", "out of range", "does not support casting", "must be type boolean",
                "is not unique", " bit string too long", "can only be updated to DEFAULT", "division by zero",
                "You might need to add explicit type casts.", "invalid regular expression",
                "View columns that are not columns of their base relation are not updatable"));
    }

    public static SQLQueryAdapter create(MaterializeGlobalState globalState) {
        return new MaterializeUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        randomTable = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        List<MaterializeColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
        sb.append("UPDATE ");
        sb.append(randomTable.getName());
        sb.append(" SET ");
        errors.add("multiple assignments to same column"); // view whose columns refer to a column in the referenced
                                                           // table multiple times
        errors.add("new row violates check option for view");
        MaterializeCommon.addCommonInsertUpdateErrors(errors);
        updateColumns(columns);
        errors.add("invalid input syntax for ");
        errors.add("operator does not exist: text = boolean");
        errors.add("violates check constraint");
        errors.add("could not determine which collation to use for string comparison");
        errors.add("but expression is of type");
        MaterializeCommon.addCommonExpressionErrors(errors);
        if (!Randomly.getBooleanWithSmallProbability()) {
            sb.append(" WHERE ");
            MaterializeExpression where = MaterializeExpressionGenerator.generateExpression(globalState,
                    randomTable.getColumns(), MaterializeDataType.BOOLEAN);
            sb.append(MaterializeVisitor.asString(where));
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    @Override
    protected void updateValue(MaterializeColumn column) {
        if (!Randomly.getBoolean()) {
            MaterializeExpression constant = MaterializeExpressionGenerator.generateConstant(globalState.getRandomly(),
                    column.getType());
            sb.append(MaterializeVisitor.asString(constant));
        } else {
            sb.append("(");
            MaterializeExpression expr = MaterializeExpressionGenerator.generateExpression(globalState,
                    randomTable.getColumns(), column.getType());
            // caused by casts
            sb.append(MaterializeVisitor.asString(expr));
            sb.append(")");
        }
    }

}
