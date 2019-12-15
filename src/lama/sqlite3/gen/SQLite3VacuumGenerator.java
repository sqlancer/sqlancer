package lama.sqlite3.gen;

import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

/**
 * @see https://www.sqlite.org/lang_vacuum.html
 */
public class SQLite3VacuumGenerator {

	public static Query executeVacuum() {
		StringBuilder sb = new StringBuilder("VACUUM");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("temp", "main"));
		}
		return new QueryAdapter(sb.toString(), Arrays.asList("cannot VACUUM from within a transaction"));
	}

}
