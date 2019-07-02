package postgres;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class PostgresReproduceVacuum {

	public static void main(String[] args) throws SQLException {
		for (int i = 0; i < 1; i++) {
			int index = i;
			
			Runnable r = new Runnable() {

				@Override
				public void run() {
					try (Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/lama" + index,
							"lama", "password")) {
						while (true) {
							try (Statement st = con.createStatement()) {
								st.execute("VACUUM FULL;");
							}
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			};
			new Thread(r).start();
		}
	}

}
