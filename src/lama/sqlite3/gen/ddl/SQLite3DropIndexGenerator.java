package lama.sqlite3.gen.ddl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.sqlite3.schema.SQLite3Schema;

// see https://www.sqlite.org/lang_dropindex.html
public class SQLite3DropIndexGenerator {

	public static Query dropIndex(Connection con, StateToReproduce state, SQLite3Schema s, Randomly r) throws SQLException {
		String indexName = s.getRandomIndexOrBailout();
		StringBuilder sb = new StringBuilder();
		sb.append("DROP INDEX ");
		if (Randomly.getBoolean()) {
			sb.append("IF EXISTS ");
		}
		sb.append('"');
		sb.append(indexName);
		sb.append('"');
		return new QueryAdapter(sb.toString(), Arrays.asList("[SQLITE_ERROR] SQL error or missing database (index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped)"), true);
	}

}
