package lama.tablegen.sqlite3;

import lama.Randomly;
import lama.sqlite3.SQLite3Visitor;

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

	public static String createIndexName(int nr) {
		return String.format("i%d", nr);
	}
	
	public static String getCheckConstraint() {
		return(" CHECK ( " + SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(false)) + ")");
	}


}
