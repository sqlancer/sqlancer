package lama.mysql;

import lama.schema.PrimitiveDataType;

public class MySQLSchemaParser {

	public static PrimitiveDataType parse(String columnType) {
		switch (columnType) {
		case "CHAR":
		case "VARCHAR":
		case "MEDIUMTEXT":
		case "TEXT":
		case "LONGTEXT":
			return PrimitiveDataType.TEXT;
		case "TIMESTAMP":
		case "TIME":
		case "DATETIME":
		case "DATE":
			return PrimitiveDataType.DATETIME;
		case "SET":
			return PrimitiveDataType.SET;
		case "ENUM":
			return PrimitiveDataType.ENUM;
		case "BIT":
		case "BIGINT":
		case "TINYINT UNSIGNED":
		case "SMALLINT UNSIGNED":
		case "BIGINT UNSIGNED":
		case "INT":
		case "INT UNSIGNED":
		case "DECIMAL":
		case "DECIMAL UNSIGNED":
			return PrimitiveDataType.INT;
		case "FLOAT":
		case "FLOAT UNSIGNED":
		case "DOUBLE UNSIGNED":
		case "DOUBLE":
			return PrimitiveDataType.REAL;
		case "LONGBLOB":
		case "BLOB":
		case "MEDIUMBLOB":
			return PrimitiveDataType.BINARY;
		}
		throw new AssertionError(columnType);
	}

}
