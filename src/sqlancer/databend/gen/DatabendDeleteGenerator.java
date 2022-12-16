package sqlancer.databend.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendToStringVisitor;

public final class DatabendDeleteGenerator {

    private DatabendDeleteGenerator() {
    }

    public static SQLQueryAdapter generate(DatabendGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(DatabendToStringVisitor.asString(DatabendExprToNode.cast(
                    new DatabendNewExpressionGenerator(globalState).generateExpression(DatabendDataType.BOOLEAN))));
            DatabendErrors.addExpressionErrors(errors);
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
