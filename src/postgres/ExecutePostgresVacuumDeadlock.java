package postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.util.PSQLException;

public class ExecutePostgresVacuumDeadlock {

	private static final String USER_NAME = "lama"; // TODO
	private static final String PASSWORD = "password"; // TODO
	private static final String DEFAULT_DATABASE = "jdbc:postgresql://localhost:5432/test";

	public static void main(String[] args) throws SQLException {

		for (int i = 0; i < 32; i++) {
			try (Connection con = DriverManager.getConnection(DEFAULT_DATABASE, USER_NAME, PASSWORD)) {
				try (Statement s = con.createStatement()) {
					s.execute("DROP DATABASE IF EXISTS test" + i);
					s.execute("CREATE DATABASE test" + i);
				}
			}
		}

		for (int i = 0; i < 32; i++) {
			final int index = i;
			Runnable r = new Runnable() {

				@Override
				public void run() {
					try {

						try (Connection con = DriverManager
								.getConnection("jdbc:postgresql://localhost:5432/test" + index, USER_NAME, PASSWORD)) {
							System.out.println("database: " + con.getCatalog());
							while (true) {
								try (Statement s = con.createStatement()) {
									s.execute(
											"CREATE UNLOGGED TABLE IF NOT EXISTS t0(c0 smallint, c1 smallint, c2 bigint GENERATED ALWAYS AS IDENTITY NULL CHECK (TRUE) );\n" + 
											"INSERT INTO t0(c1, c2, c0) VALUES(1767117542, -905029768, NULL), (-1791101302, NULL, -1128962229), (1542084583, NULL, -1982499739), (1078376734, -1891307289, 7652429);\n" + 
											"DELETE FROM ONLY t0;\n" + 
											"ANALYZE t0(c1);\n" + 
											"INSERT INTO t0(c1, c0, c2) OVERRIDING USER VALUE VALUES(NULL, -580782758, 1496600837), (814959308, NULL, NULL), (NULL, NULL, 1256558697), (NULL, NULL, 1795429856) ON CONFLICT (c0) DO NOTHING;\n" + 
											"INSERT INTO t0(c1) VALUES(DEFAULT) ON CONFLICT (c0) DO NOTHING;\n" + 
											"INSERT INTO t0(c1, c2, c0) OVERRIDING USER VALUE VALUES(745409886, -714274380, 770841957), (-586444373, NULL, -232103684), (NULL, NULL, 1873499803), (NULL, -866862866, NULL);\n" + 
											"INSERT INTO t0(c1, c2, c0) VALUES(928061869, -579641634, NULL), (NULL, -920523096, NULL) ON CONFLICT (c1) DO NOTHING;\n" + 
											"INSERT INTO t0(c2, c1) VALUES(DEFAULT, DEFAULT) ON CONFLICT  DO NOTHING;\n" + 
											"INSERT INTO t0(c0) OVERRIDING SYSTEM VALUE VALUES(612046553), (NULL), (-1943871987) ON CONFLICT  DO NOTHING;\n" + 
											"INSERT INTO t0(c2, c1, c0) VALUES(1330647279, NULL, 1401747439), (1303488818, -1274663463, 506833279), (NULL, -1314311629, NULL) ON CONFLICT  DO NOTHING;\n" + 
											"INSERT INTO t0(c1, c0, c2) OVERRIDING SYSTEM VALUE VALUES(DEFAULT, 745409886, NULL);\n" + 
											"INSERT INTO t0(c0, c2) OVERRIDING USER VALUE VALUES(NULL, -704486048), (-221367039, 208648759), (NULL, NULL), (NULL, NULL), (NULL, -878034015) ON CONFLICT (c1) DO NOTHING;\n" + 
											"REINDEX DATABASE lama5;\n" + 
											"INSERT INTO t0(c1) OVERRIDING SYSTEM VALUE VALUES(1483551459);\n" + 
											"INSERT INTO t0(c0) VALUES(NULL), (-111401073), (NULL);\n" + 
											"INSERT INTO t0(c0) OVERRIDING USER VALUE VALUES(DEFAULT) ON CONFLICT (c1) DO NOTHING;\n" + 
											"DELETE FROM t0 WHERE NULL;\n" + 
											"INSERT INTO t0(c1) VALUES(NULL), (NULL), (NULL) ON CONFLICT (c0) DO NOTHING;\n" + 
											"DISCARD PLANS;\n" + 
											"INSERT INTO t0(c2) VALUES(DEFAULT);\n" + 
											"DELETE FROM t0 WHERE NULL;\n" + 
											"INSERT INTO t0(c1) OVERRIDING SYSTEM VALUE VALUES(NULL), (NULL), (NULL), (NULL) ON CONFLICT  DO NOTHING;\n" + 
											"ANALYZE VERBOSE;\n" + 
											"ANALYZE t0(c0, c1, c2);\n" + 
											"INSERT INTO t0(c0) OVERRIDING USER VALUE VALUES(-182083875), (NULL), (NULL);\n" + 
											"DISCARD TEMPORARY;\n" + 
											"INSERT INTO t0(c2, c0, c1) OVERRIDING USER VALUE VALUES(DEFAULT, NULL, NULL);\n" + 
											"INSERT INTO t0(c1) OVERRIDING SYSTEM VALUE VALUES(NULL), (1282616210) ON CONFLICT  DO NOTHING;\n" + 
											"DELETE FROM ONLY t0 WHERE TRUE RETURNING + (NULL);\n" + 
											"DISCARD PLANS;\n" + 
											"INSERT INTO t0(c1, c0, c2) VALUES(-1164176841, 403773706, NULL), (1539855078, 698952040, 404520113) ON CONFLICT (c1) DO NOTHING;\n" + 
											"INSERT INTO t0(c0, c1, c2) OVERRIDING SYSTEM VALUE VALUES(-1164173929, 1422626267, -1299089545), (-869361086, NULL, -1747659684), (NULL, NULL, -1180765048), (1591519113, 2025743321, -1727822321) ON CONFLICT (c0) DO NOTHING;\n" + 
											"REINDEX DATABASE lama5;\n" + 
											"SET SESSION enable_gathermerge=DEFAULT;\n" + 
											"INSERT INTO t0(c0, c2) OVERRIDING USER VALUE VALUES(2083993483, -1240841430), (-102745857, NULL), (NULL, NULL), (NULL, NULL), (NULL, 1275600807);\n" + 
											"VACUUM (ANALYZE, FREEZE, FULL);");
								} catch (PSQLException e) {
									if (e.getMessage().contains("violates")) {
										
									} else {
										throw e;
									}
								}
							}
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
