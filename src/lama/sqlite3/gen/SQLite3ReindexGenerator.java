package lama.sqlite3.gen;

import java.sql.Connection;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.sqlite3.schema.SQLite3Schema;

/**
 * @see https://www.sqlite.org/lang_reindex.html
 */
public class SQLite3ReindexGenerator {

	// only works for the main schema
	public static Query executeReindex(Connection con, StateToReproduce state, SQLite3Schema s) {
		StringBuilder sb = new StringBuilder("REINDEX");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(s.getRandomTable().getName());
		} else {
			sb.append(Randomly.fromOptions("BINARY", "NOCASE", "RTRIM"));
		}
		// TODO index
		return new QueryAdapter("REINDEX;");

	}
}
