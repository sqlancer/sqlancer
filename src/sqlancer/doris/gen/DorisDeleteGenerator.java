package sqlancer.doris.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.visitor.DorisToStringVisitor;

public final class DorisDeleteGenerator extends AbstractDeleteGenerator {

    private final DorisGlobalState globalState;

    private DorisDeleteGenerator(DorisGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(DorisGlobalState globalState) {
        return new DorisDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        sb.append("DELETE FROM ");
        DorisTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(DorisToStringVisitor.asString(new DorisNewExpressionGenerator(globalState)
                    .setColumns(table.getColumns()).generateExpression(DorisSchema.DorisDataType.BOOLEAN)));
            DorisErrors.addExpressionErrors(errors);
        }
    }

}
