package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.Arrays;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public class TiDBAnalyzeTableGenerator {
	
	public static Query getQuery(TiDBGlobalState globalState) throws SQLException {
		TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
		boolean analyzeIndex = !table.getIndexes().isEmpty() && Randomly.getBoolean();
		StringBuilder sb = new StringBuilder("ANALYZE ");
		if (analyzeIndex && Randomly.getBoolean()) {
			sb.append("INCREMENTAL ");
		}
		sb.append("TABLE ");
		sb.append(table.getName());
		if (analyzeIndex) {
			sb.append(" INDEX ");
			sb.append(table.getRandomIndex().getIndexName());
		}
		if (Randomly.getBoolean()) {
			sb.append(" WITH ");
			sb.append(Randomly.getNotCachedInteger(1, 1024));
			sb.append(" BUCKETS");
		}
		return new QueryAdapter(sb.toString(), Arrays.asList("https://github.com/pingcap/tidb/issues/15993", /* https://github.com/pingcap/tidb/issues/15993 */ "doesn't have a default value"  /* https://github.com/pingcap/tidb/issues/15993 */));
	}

}
