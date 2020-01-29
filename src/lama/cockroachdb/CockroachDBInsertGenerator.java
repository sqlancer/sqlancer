package lama.cockroachdb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTable;

public class CockroachDBInsertGenerator {

	public static Query insert(CockroachDBGlobalState globalState) {
		Set<String> errors = new HashSet<>();

		CockroachDBErrors.addExpressionErrors(errors); // e.g., caused by computed columns
		errors.add("violates not-null constraint");
		errors.add("violates unique constraint");
		errors.add("primary key column");
		errors.add("cannot write directly to computed column"); // TODO: do not select generated columns

		errors.add("failed to satisfy CHECK constraint");

		errors.add("violates foreign key constraint");
		errors.add("foreign key violation");
		errors.add("multi-part foreign key");
		StringBuilder sb = new StringBuilder();
		CockroachDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
		if (Randomly.getBoolean() || true) {
			sb.append("INSERT INTO ");
		} else {
			// https://github.com/cockroachdb/cockroach/issues/44466
			sb.append("UPSERT INTO ");
		}
		sb.append(table.getName());
		sb.append(" ");
		CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState);
		if (Randomly.getBooleanWithSmallProbability()) {
			sb.append("DEFAULT VALUES");
		} else {
			List<CockroachDBColumn> columns = table.getRandomNonEmptyColumnSubset();
			sb.append("(");
			sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
			sb.append(" VALUES");
			for (int j = 0; j < Randomly.smallNumber() + 1; j++) {
				if (j != 0) {
					sb.append(", ");
				}
				sb.append("(");
				int i = 0;
				for (CockroachDBColumn c : columns) {
					if (i++ != 0) {
						sb.append(", ");
					}
					sb.append(CockroachDBVisitor.asString(gen.generateConstant(c.getColumnType())));
				}
				sb.append(")");
			}
		}
		CockroachDBErrors.addTransactionErrors(errors);
		return new QueryAdapter(sb.toString(), errors);
	}

}
