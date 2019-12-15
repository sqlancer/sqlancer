package lama.sqlite3.gen;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.sqlite3.schema.SQLite3Schema;

/**
 * @see https://www.sqlite.org/lang_reindex.html
 */
public class SQLite3ReindexGenerator {

	private enum Target {
		TABLE, INDEX, COLLATION_NAME
	}

	public static Query executeReindex(Connection con, StateToReproduce state, SQLite3Schema s) {
		StringBuilder sb = new StringBuilder("REINDEX");
		List<String> errors = new ArrayList<>();
		Target t = Randomly.fromOptions(Target.values());
		if (Randomly.getBoolean()) {
			sb.append(" ");
			switch (t) {
			case INDEX:
				sb.append(s.getRandomIndexOrBailout());
				// temp table
				errors.add("unable to identify the object to be reindexed");
				break;
			case COLLATION_NAME:
				sb.append(Randomly.fromOptions("BINARY", "NOCASE", "RTRIM"));
				break;
			case TABLE:
				sb.append(" ");
				sb.append(s.getRandomTableOrBailout(tab -> !tab.isTemp() && !tab.isView()).getName());
				break;
			}
		}
		return new QueryAdapter(sb.toString(), errors, true);
	}
}
