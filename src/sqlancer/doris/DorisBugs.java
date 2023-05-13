package sqlancer.doris;

public final class DorisBugs {
    // https://github.com/apache/doris/issues/17697
    // Logical bug about where true not in (columns)
    public static boolean bug17697 = true;

    // https://github.com/apache/doris/issues/17700
    // Cannot use between and in boolean column
    public static boolean bug17700 = true;

    // https://github.com/apache/doris/issues/17701
    // Wrong result of `where column not in (values)`
    public static boolean bug17701 = true;

    // https://github.com/apache/doris/issues/17705
    // Different result caused by `where` split and union all
    public static boolean bug17705 = true;

    // https://github.com/apache/doris/issues/19370
    // Internal Error occur in GroupBy&Having sql
    // fixed by https://github.com/apache/doris/pull/19559
    public static boolean bug19370 = true;

    // https://github.com/apache/doris/issues/19374
    // Different result of having not ($value in column) and having ($value not in column)
    // fixed by https://github.com/apache/doris/pull/19471
    public static boolean bug19374 = true;

    // https://github.com/apache/doris/issues/19611
    // ERROR occur in nested subqueries with same column name and union
    public static boolean bug19611 = true;

    // https://github.com/apache/doris/issues/19613
    // Wrong result when right outer join and where false
    public static boolean bug19613 = true;

    // https://github.com/apache/doris/issues/19614
    // Wrong result when value like column from table_join
    public static boolean bug19614 = true;

    private DorisBugs() {

    }
}
