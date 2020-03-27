package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public class TiDBIndexGenerator {

	public static Query getQuery(TiDBGlobalState globalState) throws SQLException {
		Set<String> errors = new HashSet<>();

		TiDBTable randomTable = globalState.getSchema().getRandomTable();
		String indexName = globalState.getSchema().getFreeIndexName();
		StringBuilder sb = new StringBuilder("CREATE ");
		if (Randomly.getBooleanWithRatherLowProbability()) {
			sb.append("UNIQUE ");
			errors.add("Duplicate for key");
			errors.add("Duplicate entry ");
		}
		sb.append("INDEX ");
		sb.append(indexName);
		sb.append(" ON ");
		sb.append(randomTable.getName());
		sb.append("(");
		int nr = Math.min(Randomly.smallNumber() + 1, randomTable.getColumns().size());
		List<TiDBColumn> subset = Randomly.extractNrRandomColumns(randomTable.getColumns(), nr);
		for (int i = 0; i < subset.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(subset.get(i).getName());
			TiDBTableGenerator.appendTextSizeSpecifier(sb, subset.get(i).getType().getPrimitiveDataType());
			if (Randomly.getBoolean()) {
				sb.append(" ");
				sb.append(Randomly.fromOptions("ASC", "DESC"));
			}
		}
		sb.append(")");
		if (Randomly.getBooleanWithRatherLowProbability()) {
			sb.append(" KEY_BLOCK_SIZE ");
			sb.append(Randomly.getPositiveOrZeroNonCachedInteger());
		}
		return new QueryAdapter(sb.toString(), errors, true);
	}

}
