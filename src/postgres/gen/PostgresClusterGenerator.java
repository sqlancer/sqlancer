package postgres.gen;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresSchema;
import postgres.PostgresSchema.PostgresTable;

public class PostgresClusterGenerator {

	public static Query create(PostgresSchema newSchema) {
		StringBuilder sb = new StringBuilder();
		sb.append("CLUSTER ");
		if (Randomly.getBoolean()) {
			PostgresTable table = newSchema.getRandomTable();
			sb.append(table.getName());
			if (Randomly.getBoolean() && !table.getIndexes().isEmpty()) {
				sb.append(" USING ");
				sb.append(table.getRandomIndex().getIndexName());
			}
		}
		return new QueryAdapter(sb.toString());
	}

}
