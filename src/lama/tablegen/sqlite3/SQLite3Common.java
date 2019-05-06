package lama.tablegen.sqlite3;

import lama.Randomly;

public class SQLite3Common {

	public static String getRandomCollate() {
		return Randomly.fromOptions(" COLLATE BINARY", " COLLATE RTRIM", " COLLATE NOCASE");
	}

	public static String createTableName(int nr) {
		return String.format("t%d", nr);
	}

	public static String createColumnName(int nr) {
		return String.format("c%d", nr);
	}

}
