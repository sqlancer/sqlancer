package lama.sqlite3.gen;

import java.sql.Connection;

import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;

/**
 * @see https://www.sqlite.org/lang_vacuum.html
 */
public class SQLite3VacuumGenerator {

	// only works for the main schema
	public static Query executeVacuum(Connection con, StateToReproduce state) {
		return new QueryAdapter("VACUUM;");
	}

}
