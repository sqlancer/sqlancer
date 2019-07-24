package lama.postgres.gen;

import java.util.Arrays;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresProvider;
import lama.postgres.PostgresSchema.PostgresTable;

public class PostgresAnalyzeGenerator {

	public static Query create(PostgresTable randomTable) {
		StringBuilder sb = new StringBuilder("ANALYZE");
		if (Randomly.getBoolean()) {
			sb.append("(");
			if (Randomly.getBoolean() || !PostgresProvider.IS_POSTGRES_TWELVE) {
				sb.append(" VERBOSE");
			} else {
				sb.append(" SKIP_LOCKED");
			}
			sb.append(")");
		}
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(randomTable.getName());
			if (Randomly.getBoolean()) {
				sb.append("(");
				sb.append(randomTable.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
						.collect(Collectors.joining(", ")));
				sb.append(")");
			}
		}
		// FIXME: bug in postgres?
		return new QueryAdapter(sb.toString(), Arrays.asList("deadlock"));
	}

}
