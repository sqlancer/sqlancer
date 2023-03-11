package sqlancer.doris.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.DorisToStringVisitor;

public final class DorisDeleteGenerator {

    private DorisDeleteGenerator() {
    }

    public static SQLQueryAdapter generate(DorisGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        DorisTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(DorisToStringVisitor.asString(
                    new DorisExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        DorisErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
