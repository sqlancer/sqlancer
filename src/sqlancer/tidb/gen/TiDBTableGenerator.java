package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;

public class TiDBTableGenerator {
	
	private boolean allowPrimaryKey;
	private final List<TiDBColumn> columns = new ArrayList<>();
	private boolean primaryKeyAsTableConstraints;

	public Query getQuery(TiDBGlobalState globalState) throws SQLException {
		String tableName = globalState.getSchema().getFreeTableName();
		int nrColumns = Randomly.smallNumber() + 1;
		allowPrimaryKey = Randomly.getBoolean();
		primaryKeyAsTableConstraints = allowPrimaryKey && Randomly.getBoolean();
		for (int i = 0; i < nrColumns; i++) {
			TiDBColumn fakeColumn = new TiDBColumn("c" + i, null, false, false);
			columns.add(fakeColumn);
		}
		
		StringBuilder sb = new StringBuilder("CREATE TABLE ");
		sb.append(tableName);
		sb.append("(");
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns.get(i).getName());
			sb.append(" INT "); // TODO: other types
			if (Randomly.getBooleanWithRatherLowProbability()) {
				sb.append("NOT NULL ");
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				sb.append("UNIQUE ");
			}
			if (Randomly.getBooleanWithRatherLowProbability() && allowPrimaryKey && !primaryKeyAsTableConstraints) {
				sb.append("PRIMARY KEY ");
				allowPrimaryKey = false;
			}
		}
		sb.append(")");
		return new QueryAdapter(sb.toString(), true);
		
	}
}
