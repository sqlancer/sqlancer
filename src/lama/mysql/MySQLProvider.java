package lama.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import lama.DatabaseProvider;
import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.gen.MySQLRowInserter;
import lama.mysql.gen.MySQLSetGenerator;
import lama.mysql.gen.MySQLTableGenerator;
import lama.sqlite3.gen.SQLite3Common;

public class MySQLProvider implements DatabaseProvider {

	private final Randomly r = new Randomly();

	enum Actions {
		SHOW_TABLES;
	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager) throws SQLException {

		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			String tableName = SQLite3Common.createTableName(i);
			Query createTable = MySQLTableGenerator.generate(tableName, r);
			manager.execute(createTable);
		}
		MySQLSchema newSchema = MySQLSchema.fromConnection(con, databaseName);
		for (int i = 0; i < 1000; i++) {
			Query q = MySQLRowInserter.insertRow(newSchema.getRandomTable(), r);
			manager.execute(q);
		}
		for (int i = 0; i < 1000; i++) {
			Query q = MySQLSetGenerator.set(r);
			manager.execute(q);
		}
		for (int i = 0; i < 1000; i++) {
			try (Statement s = con.createStatement()) {
				Query q = new QueryAdapter("SHOW TABLES");
				manager.execute(q);
			}
		}

	}

	@Override
	public Connection createDatabase(String databaseName) throws SQLException {
		String url = "jdbc:mysql://localhost:3306/?serverTimezone=UTC&useSSL=false";
		Connection con = DriverManager.getConnection(url, "lama", "password");
		try (Statement s = con.createStatement()) {
			s.execute("DROP DATABASE IF EXISTS " + databaseName);
		}
		try (Statement s = con.createStatement()) {
			s.execute("CREATE DATABASE " + databaseName);
		}
		try (Statement s = con.createStatement()) {
			s.execute("USE " + databaseName);
		}
		return con;
	}

}
