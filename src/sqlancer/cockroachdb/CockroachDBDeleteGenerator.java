package sqlancer.cockroachdb;

import java.util.HashSet;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;

public class CockroachDBDeleteGenerator {

	public static Query delete(CockroachDBGlobalState globalState) {
		Set<String> errors = new HashSet<>();
		StringBuilder sb = new StringBuilder();
		CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
		sb.append("DELETE FROM ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			CockroachDBErrors.addExpressionErrors(errors);
			sb.append(CockroachDBVisitor.asString(new CockroachDBExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression(CockroachDBDataType.BOOL.get())));
		} else {
			errors.add("rejected: DELETE without WHERE clause (sql_safe_updates = true)");
		}
		errors.add("foreign key violation");
		CockroachDBErrors.addTransactionErrors(errors);
		return new QueryAdapter(sb.toString(), errors);
	}
	
}
