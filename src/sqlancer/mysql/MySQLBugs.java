package sqlancer.mysql;

// do not make the fields final to avoid warnings
public final class MySQLBugs {

    // https://bugs.mysql.com/99182 BETWEEN malfunctions for DECIMAL and TEXT
    public static boolean bug99182 = true;

    // https://bugs.mysql.com/bug.php?id=99183
    public static boolean bug99183 = true;

    // https://bugs.mysql.com/bug.php?id=95894
    public static boolean bug95894 = true;

    // https://bugs.mysql.com/bug.php?id=99135
    public static boolean bug99135 = true;

    // https://bugs.mysql.com/bug.php?id=111471
    public static boolean bug111471 = true;

    // https://bugs.mysql.com/bug.php?id=112242
    public static boolean bug112242 = true;

    // https://bugs.mysql.com/bug.php?id=112243
    public static boolean bug112243 = true;

    // https://bugs.mysql.com/bug.php?id=112264
    public static boolean bug112264 = true;

    // https://bugs.mysql.com/bug.php?id=114533
    public static boolean bug114533 = true;

    // https://bugs.mysql.com/bug.php?id=114534
    public static boolean bug114534 = true;

    // https://bugs.mysql.com/bug.php?id=120710
    // Inserting a NULL and a value which rounds to 0 into a DECIMAL column causes result set mismatch. Relatedly, once
    // IFNULL(NULL, c0) on a DECIMAL column evaluates to NULL for one row, it stays NULL for all later rows when used as
    // a condition.
    public static boolean bug120710 = true;

    // https://bugs.mysql.com/bug.php?id=120711
    // Creating an index on an integer-type column, then inserting a value which rounds to 1, causes result set
    // mismatch.
    public static boolean bug120711 = true;

    // https://bugs.mysql.com/bug.php?id=120712
    // Creating an index in between two NULL inserts causes inconsistent CERT result.
    public static boolean bug120712 = true;

    // An IN list holding a literal NULL rounds its DOUBLE and string elements to integers when the left operand is an
    // integer expression, e.g., 0 IN (NULL, 0.29e0) evaluates to 1.
    public static boolean bugInNullList = true;

    // Assigning a numeric value to a text column stores a string that depends on how the value is computed rather than
    // on its type and value: a FLOAT column stores as 1836270000 but CASE WHEN 1 THEN c0 ELSE c0 END as 1836267264,
    // and a constant GREATEST(854610024, 0.5) loses the fractional digits a non-constant one keeps.
    public static boolean bugNumericToTextAssignment = true;

    // An index lookup on an integer column rounds a fractional key computed by a non-literal expression, e.g.,
    // c0 = CAST(0.3 AS DOUBLE) and c0 IN (CAST(0.3 AS DOUBLE)) match c0 = 0.
    public static boolean bugIndexLookupRounding = true;

    private MySQLBugs() {
    }

}
