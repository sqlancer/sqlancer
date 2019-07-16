package lama.mysql;
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

public class ExecuteMySQL {

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
		for (int i = 0; i < 1; i++) {
			try {
				Thread.sleep(ThreadLocalRandom.current().nextInt(0, 1000));
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			for (String fileContent : fileContents) {
				Runnable r = new Runnable() {

					@Override
					public void run() {
						while (true) {
							try (Connection con = DriverManager.getConnection(
									"jdbc:mysql://localhost:3306?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
									"lama", "password")) {
								for (String s : fileContent.split("\n")) {
									try (Statement st = con.createStatement()) {
										try {
											st.execute(s);
										} catch (Exception e) {
											if (e.getMessage().contains("Incorrect arguments to SET")) {
												System.out.println(s);
												throw e;
											}
										}
									}
								}

							} catch (SQLException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}
					}

				};
				new Thread(r).start();
			}
		}
	}
}
