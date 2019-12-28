package lama.postgres.gen;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresGlobalState;
import lama.postgres.PostgresSchema.PostgresTable;

public class PostgresClusterGenerator {

	public static Query create(PostgresGlobalState globalState) {
		StringBuilder sb = new StringBuilder();
		sb.append("CLUSTER ");
		if (Randomly.getBoolean()) {
			PostgresTable table = globalState.getSchema().getRandomTable();
			sb.append(table.getName());
			if (Randomly.getBoolean() && !table.getIndexes().isEmpty()) {
				sb.append(" USING ");
				sb.append(table.getRandomIndex().getIndexName());
			}
		}
		return new QueryAdapter(sb.toString());
	}

}
