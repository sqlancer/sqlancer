package sqlancer.duckdb.gen;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.ast.newast.Node;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBExpression;

public class DuckDBUpdateGenerator {

    private DuckDBUpdateGenerator() {
    }

    public static Query getQuery(DuckDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        Set<String> errors = new HashSet<>();
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(" SET ");
        List<DuckDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            Node<DuckDBExpression> expr;
            if (Randomly.getBooleanWithSmallProbability()) {
                expr = gen.generateExpression();
                DuckDBErrors.addExpressionErrors(errors);
            } else {
                expr = gen.generateConstant();
            }
            sb.append(DuckDBToStringVisitor.asString(expr));
        }
        DuckDBErrors.addInsertErrors(errors);
        return new QueryAdapter(sb.toString(), errors);
    }

}
