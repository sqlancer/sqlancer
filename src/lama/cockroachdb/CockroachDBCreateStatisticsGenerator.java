package lama.cockroachdb;

import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTable;

public class CockroachDBCreateStatisticsGenerator {

	public static Query create(CockroachDBGlobalState globalState) {
		CockroachDBTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView());
		StringBuilder sb = new StringBuilder("CREATE STATISTICS s");
		sb.append(Randomly.smallNumber());
		if (Randomly.getBoolean()) {
			sb.append(" ON ");
			sb.append(randomTable.getRandomColumn().getName());
		}
		sb.append(" FROM ");
		sb.append(randomTable.getName());

		Query q = new QueryAdapter(sb.toString(), Arrays.asList("current transaction is aborted, commands ignored until end of transaction block"));
		return q;
	}
	
}
