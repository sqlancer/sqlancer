package sqlancer.mysql;

public class MySQLBugs {

	// https://bugs.mysql.com/bug.php?id=99127 0.9 > t0.c0 malfunctions when c0 is
	// an INT UNSIGNED
	public static boolean BUG_99127 = true;

	// https://bugs.mysql.com/99182 BETWEEN malfunctions for DECIMAL and TEXT
	public static boolean BUG_99181 = true;

	// https://bugs.mysql.com/bug.php?id=99183
	public static boolean BUG_99183 = true;

}
