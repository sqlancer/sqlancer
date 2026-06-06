package sqlancer.h2;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Table;

public final class H2DeleteGenerator extends AbstractDeleteGenerator {

    private final H2GlobalState globalState;

    private H2DeleteGenerator(H2GlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(H2GlobalState globalState) {
        return new H2DeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        H2Table table = globalState.getSchema().getRandomTable(t -> !t.isView());
        appendDeleteFromTable(table.getName());
        if (Randomly.getBoolean()) {
            appendWhereClause(H2ToStringVisitor.asString(
                    new H2ExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        if (Randomly.getBoolean()) {
            appendLimitClause(H2ToStringVisitor.asString(new H2ExpressionGenerator(globalState).generateConstant()));
        }
        H2Errors.addExpressionErrors(errors);
        H2Errors.addDeleteErrors(errors);
    }

}
