package lama.sqlite3.gen;

import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;

/**
 * @see https://www.sqlite.org/lang_vacuum.html
 */
public class SQLite3VacuumGenerator {

	public static Query executeVacuum(SQLite3GlobalState globalState) {
		StringBuilder sb = new StringBuilder("VACUUM");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("temp", "main"));
		}
		return new QueryAdapter(sb.toString(), Arrays.asList("cannot VACUUM from within a transaction"));
	}

}
