package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.Statement;

import lama.Main.ReduceMeException;
import lama.Main.StateToReproduce;

/**
 * @see https://www.sqlite.org/lang_reindex.html
 */
public class SQLite3ReindexGenerator {

	// only works for the main schema
	public static void executeReindex(Connection con, StateToReproduce state) {
		try {
			try (Statement s = con.createStatement()) {
				state.statements.add("REINDEX;");
				// TODO select individual tables and columns
				s.execute("REINDEX;");
			}
		} catch (Throwable e) {
			state.logInconsistency(e);
			throw new ReduceMeException();
		}		
	}
}
