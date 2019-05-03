package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.Statement;

import lama.Main.ReduceMeException;
import lama.Main.StateToReproduce;

/**
 * @see https://www.sqlite.org/lang_vacuum.html
 */
public class SQLite3VacuumGenerator {

	// only works for the main schema
	public static void executeVacuum(Connection con, StateToReproduce state) {
		try {
			try (Statement s = con.createStatement()) {
				state.statements.add("VACUUM;");
				s.execute("VACUUM;");
			}
		} catch (Throwable e) {
			state.logInconsistency(e);
			throw new ReduceMeException();
		}		
	}

}
