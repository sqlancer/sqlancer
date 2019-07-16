package lama.postgres;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ExecuteVacuumOverflow {

	public static void main(String[] args) throws SQLException {
		List<File> list = Arrays.asList(new File(".").listFiles(new FilenameFilter() {
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
//			try {
//				Thread.sleep(ThreadLocalRandom.current().nextInt(0, 1000));
//			} catch (InterruptedException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
		int fileNr = 0;
		Pattern p = Pattern.compile("\\\\c (.*);");
		for (String fileContent : fileContents) {
			Runnable r = new Runnable() {

				@Override
				public void run() {
					try {
						while (true) {
						Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test",
								"lama", "password");
							for (String s : fileContent.split("\n")) {
								Matcher matcher = p.matcher(s);
								if (matcher.matches()) {
									String databaseName = matcher.group(1);
									con.close();
									con = DriverManager.getConnection(
											"jdbc:postgresql://localhost:5432/" + databaseName, "lama", "password");
								}
								try (Statement st = con.createStatement()) {
//									Thread.sleep(ThreadLocalRandom.current().nextInt(0, 100));
									try {
										st.execute(s);
									} catch (Exception e) {
										if (e.getMessage().contains("invalid input syntax for type boolean") && s.contains("VACUUM")) {
											throw new AssertionError(e);
										} else if (e.getMessage().contains("deadlock") && !s.contains("VACUUM")) {
											throw new AssertionError(s, e);
										} else if (e.getMessage().contains("relation mapping")) {
											throw new AssertionError(s, e);
										} else if (e.getMessage().contains("incorrect checksum")) {
											throw new AssertionError(s, e);
										} else if (e.getMessage().contains("unexpected null")) {
											throw new AssertionError(s, e);
										} else if (e.getMessage().contains("integer out of range") && s.contains("VACUUM")) {
											throw new AssertionError(s, e);
										}
									}
								}
							}
							con.close();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			};
			new Thread(r).start();
		}
	}
}
