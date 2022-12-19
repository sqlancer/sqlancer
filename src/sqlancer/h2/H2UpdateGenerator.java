package sqlancer.h2;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2Table;

public final class H2UpdateGenerator extends AbstractUpdateGenerator<H2Column> {

    private final H2GlobalState globalState;
    private H2ExpressionGenerator gen;

    private H2UpdateGenerator(H2GlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(H2GlobalState globalState) {
        return new H2UpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        H2Table table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<H2Column> columns = table.getRandomNonEmptyColumnSubset();
        gen = new H2ExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        H2Errors.addInsertErrors(errors);
        H2Errors.addDeleteErrors(errors);
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(H2ToStringVisitor.asString(gen.generateExpression()));
        }
        H2Errors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(H2Column column) {
        sb.append(H2ToStringVisitor.asString(gen.generateConstant()));
    }

}
