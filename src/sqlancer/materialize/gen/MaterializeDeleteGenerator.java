package sqlancer.materialize.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeVisitor;

public final class MaterializeDeleteGenerator {

    private MaterializeDeleteGenerator() {
    }

    public static SQLQueryAdapter create(MaterializeGlobalState globalState) {
        MaterializeTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("violates foreign key constraint");
        errors.add("violates not-null constraint");
        errors.add("could not determine which collation to use for string comparison");
        StringBuilder sb = new StringBuilder("DELETE FROM");
        sb.append(" ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(MaterializeVisitor.asString(MaterializeExpressionGenerator.generateExpression(globalState,
                    table.getColumns(), MaterializeDataType.BOOLEAN)));
        }
        MaterializeCommon.addCommonExpressionErrors(errors);
        errors.add("out of range");
        errors.add("does not support casting");
        errors.add("invalid input syntax for");
        errors.add("division by zero");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
