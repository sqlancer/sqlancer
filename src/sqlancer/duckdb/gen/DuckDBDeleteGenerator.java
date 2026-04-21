package sqlancer.duckdb.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

public final class DuckDBDeleteGenerator extends AbstractDeleteGenerator {

    private final DuckDBGlobalState globalState;

    private DuckDBDeleteGenerator(DuckDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(DuckDBGlobalState globalState) {
        return new DuckDBDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        sb.append("DELETE FROM ");
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(DuckDBToStringVisitor.asString(
                    new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        DuckDBErrors.addExpressionErrors(errors);
    }

}
