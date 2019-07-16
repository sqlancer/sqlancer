package lama.mysql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TriggerSetKeyCacheDivisionLimitBug {

	private static final String PASSWORD = "password";
	private static final String USER_NAME = "lama";
	private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";

	public static void main(String[] args) {
		while (true) {
			try (Connection con = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD)) {
				try (Statement st = con.createStatement()) {
					// should also work with key_cache_age_threshold, key_cache_block_size,
					// key_cache_division_limit and key_buffer_size global variables
					st.execute("SET SESSION key_cache_division_limit = 100");
					st.execute("SELECT 1;"); // if the bug cannot be reproduced, try DO SLEEP(1); instead
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}

}
