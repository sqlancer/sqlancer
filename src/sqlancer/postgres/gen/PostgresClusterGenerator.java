package sqlancer.postgres.gen;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;

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
