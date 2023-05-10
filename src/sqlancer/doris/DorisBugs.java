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
    public static boolean bug19370 = true;

    // https://github.com/apache/doris/issues/19374
    // Different result of having not ($value in column) and having ($value not in column)
    public static boolean bug19374 = true;

    private DorisBugs() {

    }
}
