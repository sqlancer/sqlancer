package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBDeleteGenerator {

	public static Query getQuery(TiDBGlobalState globalState) throws SQLException {
		Set<String> errors = new HashSet<>();
		TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
		TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(table.getColumns());
		StringBuilder sb = new StringBuilder("DELETE ");
		if (Randomly.getBooleanWithSmallProbability()) {
			sb.append("LOW_PRIORITY ");
		}
		if (Randomly.getBooleanWithSmallProbability()) {
			sb.append("QUICK ");
		}
		if (Randomly.getBooleanWithSmallProbability()) {
			sb.append("IGNORE ");
		}
		sb.append("FROM ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			sb.append(TiDBVisitor.asString(gen.generateExpression()));
			errors.add("Truncated incorrect");
			errors.add("Data truncation");
		}
		if (Randomly.getBoolean()) {
			sb.append(" ORDER BY ");
			TiDBErrors.addExpressionErrors(errors);
			sb.append(gen.generateOrderBys().stream().map(o -> TiDBVisitor.asString(o)).collect(Collectors.joining(", ")));
		}
		if (Randomly.getBoolean()) {
			sb.append(" LIMIT ");
			sb.append(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE));
		}
		return new QueryAdapter(sb.toString(), errors);
		
	}
	
}
