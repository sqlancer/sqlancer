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
	
	public static Query explain(SQLite3Schema newSchema, Connection con, SQLite3StateToReproduce state, Randomly r) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("EXPLAIN ");
		if (Randomly.getBoolean()) {
			sb.append("QUERY PLAN ");
		}
		Action action;
		while (true) {
			action = Randomly.fromOptions(SQLite3Provider.Action.values());
			if (action != Action.EXPLAIN) {
				break;
			}
		}
		sb.append(action.getQuery(newSchema, con, state, r));
		return new QueryAdapter(sb.toString());
	}

}
