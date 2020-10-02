package sqlancer.tidb;

// do not make the fields final to avoid warnings
public final class TiDBBugs {

    // https://github.com/pingcap/tidb/issues/15987
    public static boolean bug15987 = true;

    // // https://github.com/pingcap/tidb/issues/15988
    public static boolean bug15988 = true;

    // https://github.com/pingcap/tidb/issues/16028
    public static boolean bug16028 = true;

    // https://github.com/pingcap/tidb/issues/16020
    public static boolean bug16020 = true;

    // https://github.com/pingcap/tidb/issues/15990
    public static boolean bug15990 = true;

    // https://github.com/pingcap/tidb/issues/15844
    public static boolean bug15844 = true;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/10
    public static boolean bug10 = true;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/14
    public static boolean bug14 = true;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/15
    public static boolean bug15 = true;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/16
    public static boolean bug16 = true;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/19
    public static boolean bug19 = true;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/48
    public static boolean bug48 = true;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/50
    public static boolean bug50 = true;

    // https://github.com/citusdata/citus/issues/4079
    public static boolean bug4079 = true;

    private TiDBBugs() {
    }

}
