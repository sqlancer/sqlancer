package sqlancer.h2;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2Table;

public final class H2UpdateGenerator {

    private H2UpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(H2GlobalState globalState) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        ExpectedErrors errors = new ExpectedErrors();
        H2Table table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        H2ExpressionGenerator gen = new H2ExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(" SET ");
        List<H2Column> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            sb.append(H2ToStringVisitor.asString(gen.generateConstant()));
        }
        H2Errors.addInsertErrors(errors);
        H2Errors.addDeleteErrors(errors);
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(H2ToStringVisitor.asString(gen.generateExpression()));
        }
        H2Errors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
