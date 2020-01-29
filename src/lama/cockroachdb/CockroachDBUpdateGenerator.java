package lama.cockroachdb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import lama.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTable;

public class CockroachDBUpdateGenerator {
	
	public static Query gen(CockroachDBGlobalState globalState) {
		Set<String> errors = new HashSet<String>();
		CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
		List<CockroachDBColumn> columns = table.getRandomNonEmptyColumnSubset();
		CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState).setColumns(columns);
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(table.getName());
		sb.append(" SET ");
		int i = 0;
		for (CockroachDBColumn c : columns) {
			if (i++ != 0) {
				sb.append(", ");
			}
			sb.append(c.getName());
			sb.append("=");
			sb.append(CockroachDBVisitor.asString(gen.generateExpression(c.getColumnType())));
		}
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			sb.append(CockroachDBVisitor.asString(gen.generateExpression(CockroachDBDataType.BOOL.get())));
		}
		errors.add("violates unique constraint");
		errors.add("violates not-null constraint");
		errors.add("violates foreign key constraint");
		errors.add("UPDATE without WHERE clause (sql_safe_updates = true)");
		errors.add("numeric constant out of int64 range");
		errors.add("failed to satisfy CHECK constraint");
		CockroachDBErrors.addExpressionErrors(errors);
		CockroachDBErrors.addTransactionErrors(errors);
		return new QueryAdapter(sb.toString(), errors);
	}

}
