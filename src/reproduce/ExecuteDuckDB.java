package reproduce;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ExecuteDuckDB {

	public static void main(String[] args) throws SQLException, IOException {
		System.out.println(new File("logs/DuckDB").getAbsolutePath());
		List<File> list = Arrays.asList(new File("logs/DuckDB").listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".log"); // or something else
			}
		}));
		System.out.println(list);
		List<String> fileContents = list.stream().map(file -> {
			try {
				return new String(Files.readAllBytes(Paths.get(file.toURI())));
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		}).collect(Collectors.toList());
		FileWriter logFileWriter = new FileWriter("test");
		while (true) {
			for (String fileContent : fileContents) {
				Runnable r = new Runnable() {

					@Override
					public void run() {
						try {
							try {
								Class.forName("nl.cwi.da.duckdb.DuckDBDriver");
							} catch (ClassNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							String url = "jdbc:duckdb:";
							Connection con = DriverManager.getConnection(url, "sqlancer", "sqlancer");
							for (String s : fileContent.split(";")) {

								try (Statement st = con.createStatement()) {
									System.out.println(s);
									if (s.startsWith("SELECT")) {
										
										ResultSet rs = st.executeQuery(s);
										while (rs.next()) {
											System.out.println(rs.getString(1));
										}
									} else {
										st.execute(s);
									}
								} catch (Exception e) {
									
								}
							}
							con.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				};
				r.run();
			}
		}
	}
}
