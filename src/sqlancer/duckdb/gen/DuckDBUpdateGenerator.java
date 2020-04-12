package sqlancer.duckdb.gen;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.IgnoreMeException;
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
	
	public static Query getQuery(DuckDBGlobalState globalState) {
		if (true) {
			// https://github.com/cwida/duckdb/issues/534
			throw new IgnoreMeException();
		}
		StringBuilder sb = new StringBuilder("UPDATE ");
		DuckDBTable table = globalState.getSchema().getRandomTable();
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
			} else {
				expr = gen.generateConstant();
			}
			sb.append(DuckDBToStringVisitor.asString(expr));
		}
		Set<String> errors = new HashSet<>();
		DuckDBErrors.addInsertErrors(errors);
		return new QueryAdapter(sb.toString(), errors);
	}


}
