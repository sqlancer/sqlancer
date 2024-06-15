package sqlancer.doris;

public final class DorisBugs {
    // https://github.com/apache/doris/issues/19370
    // Internal Error occur in GroupBy&Having sql
    // fixed by https://github.com/apache/doris/pull/19559
    public static boolean bug19370;

    // https://github.com/apache/doris/issues/19374
    // Different result of having not ($value in column) and having ($value not in column)
    // fixed by https://github.com/apache/doris/pull/19471
    public static boolean bug19374;

    // https://github.com/apache/doris/issues/19611
    // ERROR occur in nested subqueries with same column name and union
    public static boolean bug19611 = true;

    // https://github.com/apache/doris/issues/36070
    // Expression evaluate to NULL but is treated as FALSE in where clause
    public static boolean bug36070 = true;

    // https://github.com/apache/doris/issues/36072
    // SELECT DISTINCT does not work with aggregate key column
    public static boolean bug36072 = true;

    // https://github.com/apache/doris/issues/36342
    // Wrong result with INNER JOIN and CURRENT_TIMESTAMP
    public static boolean bug36342 = true;

    // https://github.com/apache/doris/issues/36343
    // Wrong result with SELECT DISTINCT and UNIQUE model
    public static boolean bug36343 = true;

    // https://github.com/apache/doris/issues/36346
    // Wrong result with LEFT JOIN SELECT DISTINCT and IN operation
    public static boolean bug36346 = true;

    private DorisBugs() {

    }
}
