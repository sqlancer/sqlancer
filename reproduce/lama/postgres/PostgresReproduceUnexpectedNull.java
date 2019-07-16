package lama.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

public class PostgresReproduceUnexpectedNull {

	private static final int NR_THREADS = 32;
	private static final String USER_NAME = "";
	private static final String PASSWORD = "";

	public static void main(String[] args) throws SQLException {
		for (int i = 0; i < NR_THREADS; i++) {
			final int index = i;

			Runnable r = new Runnable() {

				@Override
				public void run() {
					while (true) {
						try (Connection con = DriverManager
								.getConnection("jdbc:postgresql://localhost:5432/test" + index, USER_NAME, PASSWORD)) {
							try (Statement createStatement = con.createStatement()) {
								createStatement.execute("DROP DATABASE IF EXISTS d" + index);
								createStatement.execute("CREATE DATABASE d" + index);
							}
						} catch (SQLException e1) {
							throw new AssertionError(e1);
						}
						try (Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/d" + index,
								USER_NAME, PASSWORD)) {
							String statements = 
									"CREATE TABLE t0(c0 TEXT);\n" +
									"INSERT INTO t0(c0) VALUES('b'), ('a');\n" +
									"ANALYZE;\n" +
									"INSERT INTO t0(c0) VALUES (NULL);\n" +
									"UPDATE t0 SET c0 = 'a';\n" +
									"CREATE INDEX i0 ON t0(c0);\n" +
									"SELECT * FROM t0 WHERE 'baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' > t0.c0;";
							for (String s : statements.split("\n")) {
								try (Statement st = con.createStatement()) {
									try {
										Thread.sleep(ThreadLocalRandom.current().nextInt(0, 100));
									} catch (InterruptedException e) {
										throw new AssertionError(e);
									}
									st.execute(s);
								}
							}
						} catch (SQLException e) {
							throw new AssertionError(e);
						}

					}
				}

			};
			new Thread(r).start();
		}
	}

}
