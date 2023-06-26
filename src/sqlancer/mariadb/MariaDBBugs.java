package sqlancer.mariadb;

public final class MariaDBBugs {

    // https://jira.mariadb.org/browse/MDEV-21058
    public static boolean bug21058 = true;

    // https://github.com/sqlancer/sqlancer/pull/834
    public static boolean bug835 = true;

    private MariaDBBugs() {
    }

}
