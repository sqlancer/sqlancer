package sqlancer.yugabyte.ycql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLErrors;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;

public final class YCQLDeleteGenerator {

    private YCQLDeleteGenerator() {
    }

    public static SQLQueryAdapter generate(YCQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(YCQLToStringVisitor.asString(
                    new YCQLExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }

        YCQLErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
