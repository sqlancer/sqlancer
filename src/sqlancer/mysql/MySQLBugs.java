package sqlancer.mysql;

// do not make the fields final to avoid warnings
public final class MySQLBugs {

    // https://bugs.mysql.com/bug.php?id=99127 0.9 > t0.c0 malfunctions when c0 is
    // an INT UNSIGNED
    public static boolean bug99127 = true;

    // https://bugs.mysql.com/99182 BETWEEN malfunctions for DECIMAL and TEXT
    public static boolean bug99181 = true;

    // https://bugs.mysql.com/bug.php?id=99183
    public static boolean bug99183 = true;

    // https://bugs.mysql.com/bug.php?id=95894
    public static boolean bug95894 = true;

    // https://bugs.mysql.com/bug.php?id=99135
    public static boolean bug99135 = true;

    // https://bugs.mysql.com/bug.php?id=111471
    public static boolean bug111471 = true;

    private MySQLBugs() {
    }

}
