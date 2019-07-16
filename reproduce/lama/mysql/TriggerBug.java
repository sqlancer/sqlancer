package lama.mysql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TriggerBug {

	private static final String USER_NAME = "lama";
	private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";
	private static final String PASSWORD = "password";

	public static void main(String[] args) {
		String query = "DROP DATABASE IF EXISTS triggerbug%d;\n" + "CREATE DATABASE triggerbug%d;\n"
				+ "USE triggerbug%d;\n" + "CREATE TABLE t0(c0 INT) ENGINE = MyISAM;\n"
				+ "select * from information_schema.TABLES;\n";

		for (int i = 0; i < 32; i++) {
			final int index = i;
			Runnable r = new Runnable() {

				@Override
				public void run() {

					try (Connection con = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD)) {
						while (true) {
							for (String s : String.format(query, index, index, index).split("\n")) {
								try (Statement st = con.createStatement()) {
									st.execute(s);
								}
							}

						}
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}

			};
			new Thread(r).start();
		}
	}

}
