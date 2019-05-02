package lama;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import lama.QueryGenerator.Database;
import lama.schema.PrimitiveDataType;
import lama.schema.Schema.Table;
import lama.sqlite3.SQLite3SchemaParser;

public class DatabaseFacade {
	
	
	public static Connection createDatabase(String fileName) throws SQLException {
		assert QueryGenerator.DATABASE == Database.SQLITE;
		File dataBase = new File("." + File.separator + "databases", fileName + ".db");
		if (dataBase.exists()) {
			dataBase.delete();
		}
        String url = "jdbc:sqlite:" + dataBase.getAbsolutePath();
        return DriverManager.getConnection(url);
	}

	public static Connection getConnection() throws SQLException {
		switch (QueryGenerator.DATABASE) {
		case MYSQL:
			String url = "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC";
			return DriverManager.getConnection(url, "lama", "lamalama123!");
		case SQLITE:
			return DriverManager.getConnection("jdbc:sqlite:databases/chinook.db");
		default:
			throw new AssertionError();
		}
	}

	public static void getColumnType(String columnType) {
	}

	public static PrimitiveDataType parseColumnType(String columnType) {
		switch (QueryGenerator.DATABASE) {
		case MYSQL:
			return lama.mysql.MySQLSchemaParser.parse(columnType);
		case SQLITE:
			return SQLite3SchemaParser.parse(columnType);
		default:
			throw new AssertionError();
		}
	}

	public static String queryStringToGetRandomTableRow(Table table) {
		switch (QueryGenerator.DATABASE) {
		case MYSQL: 
			return "SELECT * FROM " + table + " ORDER BY RAND() LIMIT 1";
		case SQLITE:
			return String.format("SELECT %s, %s FROM %s ORDER BY RANDOM() LIMIT 1", table.getColumnsAsString(), table.getColumnsAsString(c -> "typeof(" + c.getName() + ")" ), table.getName());
		default:
			throw new AssertionError();
		}
	}

}
