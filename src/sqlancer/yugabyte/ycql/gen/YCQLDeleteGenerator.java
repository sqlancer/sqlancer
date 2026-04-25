package sqlancer.yugabyte.ycql.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLErrors;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;

public final class YCQLDeleteGenerator extends AbstractDeleteGenerator {

    private final YCQLGlobalState globalState;

    private YCQLDeleteGenerator(YCQLGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(YCQLGlobalState globalState) {
        return new YCQLDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        appendDeleteFromTable(table.getName());
        if (Randomly.getBoolean()) {
            appendWhereClause(YCQLToStringVisitor.asString(
                    new YCQLExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        YCQLErrors.addExpressionErrors(errors);
    }

}
