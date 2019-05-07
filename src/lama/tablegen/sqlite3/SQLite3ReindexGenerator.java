package lama.tablegen.sqlite3;

import java.sql.Connection;

import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;

/**
 * @see https://www.sqlite.org/lang_reindex.html
 */
public class SQLite3ReindexGenerator {

	// only works for the main schema
	public static Query executeReindex(Connection con, StateToReproduce state) {
		return new QueryAdapter("REINDEX;");
	}
}
