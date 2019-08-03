package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.SQLite3Provider;
import lama.sqlite3.SQLite3Provider.Action;
import lama.sqlite3.schema.SQLite3Schema;

public class SQLite3ExplainGenerator {

	public static Query explain(Connection con, SQLite3StateToReproduce state, Randomly r)
			throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("EXPLAIN ");
		SQLite3Schema newSchema = SQLite3Schema.fromConnection(con);
		if (Randomly.getBoolean()) {
			sb.append("QUERY PLAN ");
		}
		Action action;
		do {
			action = Randomly.fromOptions(SQLite3Provider.Action.values());
			if (action == Action.EXPLAIN) {
				continue;
			}
			if (action == Action.TARGETED_SELECT
					&& newSchema.getDatabaseTables().stream().anyMatch(t -> t.getNrRows() == 0)) {
				continue;
			}
			break;
		} while (true);
		Query query = action.getQuery(newSchema, con, state, r);
		sb.append(query);
		return new QueryAdapter(sb.toString(), query.getExpectedErrors());
	}

}
