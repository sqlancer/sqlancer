package postgres.gen;

import java.util.List;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresSchema;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresTable;

public class PostgresInsertStatisticsGenerator {

	public static Query insert(PostgresSchema newSchema, Randomly r) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE STATISTICS ");
		if (Randomly.getBoolean()) {
			sb.append(" IF NOT EXISTS");
		}
		sb.append(" s1");
		// TODO statistics kinds
		PostgresTable randomTable = newSchema.getRandomTable();
		if (randomTable.getColumns().size() < 2) {
			throw new IgnoreMeException();
		}
		List<PostgresColumn> randomColumns = randomTable.getRandomNonEmptyColumnSubset(r.getInteger(2, randomTable.getColumns().size()));
		sb.append(" ON ");
		sb.append(randomColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
		sb.append(" FROM ");
		sb.append(randomTable.getName());
		return new QueryAdapter(sb.toString());
	}

}
