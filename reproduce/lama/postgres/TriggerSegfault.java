package lama.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

public class TriggerSegfault {

	private static final String PASSWORD = "";
	private static final String USER_NAME = "";
	static String statements = "CREATE TABLE t0(c1 integer);\n" + //
			"INSERT INTO t0(c1) VALUES(0), (0), (0), (0), (0);\n" + //
			"BEGIN;\n" + //
			"DELETE FROM t0;\n" + "INSERT INTO t0(c1) VALUES (0);\n" + //
			"ANALYZE;";

	private static void dropCreateDatabase(String db) {
		try (Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", USER_NAME,
				PASSWORD)) {
			try (Statement s = con.createStatement()) {
				s.execute(String.format("DROP DATABASE IF EXISTS %s;", db));
				s.execute(String.format("CREATE DATABASE %s;", db));
			}
		} catch (SQLException e) {
		}
	}

	public static void main(String[] args) {
		for (int i = 0; i < 16; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						dropCreateDatabase("asdf");
					}
				}
			}).start();
		}
		for (int i = 0; i < 16; i++) {
			Runnable r2 = new Runnable() {

				@Override
				public void run() {
					while (true) {
						dropCreateDatabase("db");
						try (Connection con2 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db",
								USER_NAME, PASSWORD)) {
							for (String statement : statements.split("\n")) {
								try (Statement s = con2.createStatement()) {
									s.execute(statement);
								} catch (SQLException e) {
									if (!e.getMessage().contains("duplicate key value violates unique constraint")
											&& !e.getMessage().contains("already exists")) {
										e.printStackTrace();
									}
								}
							}
						} catch (SQLException e) {
							// ignore
						}
					}

				}
			};
			new Thread(r2).start();
		}
	}

}
