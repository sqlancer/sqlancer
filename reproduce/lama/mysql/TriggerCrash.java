package lama.mysql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

public class TriggerCrash {

	private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";
	private static final String USER_NAME = "";
	private static final String PASSWORD = "";

	public static void main(String[] args) {
		String query = "DROP DATABASE IF EXISTS db;\n" + 
				"CREATE DATABASE db;\n" + 
				"USE db;\n" + 
				"CREATE TABLE t0(c0 INT);\n" + 
				"CREATE INDEX i0 ON t0((t0.c0 || 1));\n" + 
				"INSERT INTO t0(c0) VALUES(1);\n" + 
				"CHECK TABLE t0 FOR UPGRADE;";

		for (int i = 0; i < 32; i++) {
			Runnable r = new Runnable() {

				@Override
				public void run() {

					try (Connection con = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD)) {
						while (true) {
							try {
								Thread.sleep(ThreadLocalRandom.current().nextInt(0, 1000));
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
							for (String s : query.split("\n")) {
								try (Statement st = con.createStatement()) {
									st.execute(s);
								} catch (Exception e) {
									// ignore
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
