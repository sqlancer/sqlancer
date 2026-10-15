package sqlancer.cloudberry;

// do not make the fields final to avoid warnings
public final class CloudberryBugs {

    // https://github.com/apache/cloudberry/issues/2126
    public static boolean bug2126 = true;

    // https://github.com/apache/cloudberry/issues/2134
    public static boolean bug2134 = true;

    // https://github.com/apache/cloudberry/issues/2135
    public static boolean bug2135 = true;

    // https://github.com/apache/cloudberry/issues/2137
    public static boolean bug2137 = true;

    // In Cloudberry 2.1.0, which the CI job installs, a database-wide VACUUM FULL or a non-concurrent REINDEX DATABASE,
    // which also process the shared catalogs, can deadlock with a concurrent DROP DATABASE, after which no new
    // connection succeeds. Not reproducible on main; remove once CI uses a newer release.
    public static boolean bugSharedCatalogDropDatabaseDeadlock = true;

    // https://github.com/apache/cloudberry/pull/1941
    public static boolean bug1941 = true;

    // Cloudberry 2.1.0, which the CI job installs, can raise "global xmin (...) is higher than transaction xmin
    // (...)". The check no longer exists on main; remove once CI uses a newer release.
    public static boolean bugGlobalXminHigherThanTransactionXmin = true;

    private CloudberryBugs() {
    }

}
