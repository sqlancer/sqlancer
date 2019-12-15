package lama.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresProvider;
import lama.postgres.PostgresSchema;
import lama.postgres.PostgresSchema.PostgresIndex;

public class PostgresReindexGenerator {
	
	private enum Scope {
		INDEX, TABLE, DATABASE;
	}

	public static Query create(PostgresSchema newSchema) {
		List<String> errors = new ArrayList<>();
		errors.add("could not create unique index"); // CONCURRENT INDEX
		StringBuilder sb = new StringBuilder();
		sb.append("REINDEX");
//		if (Randomly.getBoolean()) {
//			sb.append(" VERBOSE");
//		}
		sb.append(" ");
		Scope scope = Randomly.fromOptions(Scope.values());
		switch (scope) {
		case INDEX:
			sb.append("INDEX ");
			if (PostgresProvider.IS_POSTGRES_TWELVE && Randomly.getBoolean()) {
				sb.append("CONCURRENTLY ");
			}
			List<PostgresIndex> indexes = newSchema.getRandomTable().getIndexes();
			if (indexes.isEmpty()) {
				throw new IgnoreMeException();
			}
			sb.append(indexes.stream().map(i -> i.getIndexName()).collect(Collectors.joining()));
			break;
		case TABLE:
			sb.append("TABLE ");
			if (PostgresProvider.IS_POSTGRES_TWELVE && Randomly.getBoolean()) {
				sb.append("CONCURRENTLY ");
			}
			sb.append(newSchema.getRandomTable(t -> !t.isView()).getName());
			break;
		case DATABASE:
			sb.append("DATABASE ");
			if (PostgresProvider.IS_POSTGRES_TWELVE && Randomly.getBoolean()) {
				sb.append("CONCURRENTLY ");
			}
			sb.append(newSchema.getDatabaseName());
			break;
		default:
			throw new AssertionError(scope);
		}
		errors.add("already contains data"); // FIXME bug report
		errors.add("does not exist"); // internal index
		errors.add("REINDEX is not yet implemented for partitioned indexes");
		return new QueryAdapter(sb.toString(), errors);
	}

}
