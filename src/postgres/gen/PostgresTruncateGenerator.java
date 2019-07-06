package postgres.gen;

import java.util.Arrays;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresSchema;

public class PostgresTruncateGenerator {

	public static Query create(PostgresSchema newSchema) {
		StringBuilder sb = new StringBuilder();
		sb.append("TRUNCATE");
		if (Randomly.getBoolean()) {
			sb.append(" TABLE");
		}
		// TODO partitions
//		if (Randomly.getBoolean()) {
//			sb.append(" ONLY");
//		}
		sb.append(" ");
		sb.append(newSchema.getDatabaseTablesRandomSubsetNotEmpty().stream().map(t -> t.getName())
				.collect(Collectors.joining(", ")));
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("RESTART IDENTITY", "CONTINUE IDENTITY"));
		}
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
		}
		return new QueryAdapter(sb.toString(), Arrays.asList("cannot truncate a table referenced in a foreign key constraint"));
	}

}
