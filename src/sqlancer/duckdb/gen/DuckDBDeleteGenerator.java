package sqlancer.duckdb.gen;

import sqlancer.ExpectedErrors;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

public final class DuckDBDeleteGenerator {

    private DuckDBDeleteGenerator() {
    }

    public static Query generate(DuckDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(DuckDBToStringVisitor.asString(
                    new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        DuckDBErrors.addExpressionErrors(errors);
        return new QueryAdapter(sb.toString(), errors);
    }

}
