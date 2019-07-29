package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;

import org.sqlite.SQLiteException;

import lama.Query;
import lama.QueryAdapter;

/**
 * @see https://www.sqlite.org/lang_vacuum.html
 */
public class SQLite3VacuumGenerator {

	// only works for the main schema
	public static Query executeVacuum() {
		return new QueryAdapter("VACUUM;") {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLiteException e) {
					if (!e.getMessage().contentEquals("[SQLITE_ERROR] SQL error or missing database (cannot VACUUM from within a transaction)")) {
						throw e;
					}
				}
			}
		};
	}

}
