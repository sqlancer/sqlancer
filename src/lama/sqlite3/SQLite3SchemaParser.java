package lama.sqlite3;

import lama.schema.PrimitiveDataType;

public class SQLite3SchemaParser {

	public static PrimitiveDataType parse(String datatype) {
		datatype = datatype.toUpperCase();
		switch (datatype) {
		case "TEXT":
			return PrimitiveDataType.TEXT;
		case "INTEGER":
		case "INT":
			return PrimitiveDataType.INT;
		case "DATETIME":
			return PrimitiveDataType.DATETIME;
		case "":
			return PrimitiveDataType.NONE;
		case "BLOB":
			return PrimitiveDataType.BINARY;
		case "REAL":
			return PrimitiveDataType.REAL;
		case "NULL":
			return PrimitiveDataType.NULL;
		default:
			throw new AssertionError(datatype);
		}
	}

}
