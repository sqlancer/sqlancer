package sqlancer.duckdb.gen;

import java.util.HashSet;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBDeleteGenerator {

    public static Query generate(DuckDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        Set<String> errors = new HashSet<>();
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
