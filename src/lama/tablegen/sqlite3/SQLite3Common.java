package lama.tablegen.sqlite3;

import lama.Randomly;

public class SQLite3Common {

	public static String getRandomCollate() {
		return Randomly.fromOptions(" COLLATE BINARY", " COLLATE RTRIM", " COLLATE NOCASE");
	}
	
}
