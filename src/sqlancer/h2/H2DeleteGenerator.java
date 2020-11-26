package sqlancer.h2;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Table;

public final class H2DeleteGenerator {

    private H2DeleteGenerator() {
    }

    public static SQLQueryAdapter getQuery(H2GlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        H2Table table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(H2ToStringVisitor.asString(
                    new H2ExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        if (Randomly.getBoolean()) {
            sb.append(" LIMIT ");
            sb.append(H2ToStringVisitor.asString(new H2ExpressionGenerator(globalState).generateConstant()));
        }
        H2Errors.addExpressionErrors(errors);
        H2Errors.addDeleteErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
