package lama.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ExecutePostgresUniqueConstraint {

	private static final String USER_NAME = "lama"; // TODO
	private static final String PASSWORD = "password"; // TODO
	private static final String DEFAULT_DATABASE = "jdbc:postgresql://localhost:5432/test";

	public static void main(String[] args) throws SQLException {

		while (true) {
			for (int i = 0; i < 32; i++) {
				try (Connection con = DriverManager.getConnection(DEFAULT_DATABASE, USER_NAME, PASSWORD)) {
					try (Statement s = con.createStatement()) {
						s.execute("DROP DATABASE IF EXISTS test" + i);
						s.execute("CREATE DATABASE test" + i);
					}
				}
				try (Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test" + i,
						USER_NAME, PASSWORD)) {
					con.createStatement().execute("SELECT 1");
				}
			}
		}
	}

}
