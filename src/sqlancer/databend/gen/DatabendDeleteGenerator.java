package sqlancer.databend.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendToStringVisitor;

public final class DatabendDeleteGenerator extends AbstractDeleteGenerator {

    private final DatabendGlobalState globalState;

    private DatabendDeleteGenerator(DatabendGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(DatabendGlobalState globalState) {
        return new DatabendDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        sb.append("DELETE FROM ");
        sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(DatabendToStringVisitor.asString(
                    new DatabendNewExpressionGenerator(globalState).generateExpression(DatabendDataType.BOOLEAN)));
            DatabendErrors.addExpressionErrors(errors);
        }
    }

}
