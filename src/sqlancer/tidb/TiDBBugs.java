package sqlancer.tidb;

// do not make the fields final to avoid warnings
public final class TiDBBugs {

    // https://github.com/pingcap/tidb/issues/15987
    public static boolean bug15987 = false;

    // // https://github.com/pingcap/tidb/issues/15988
    public static boolean bug15988 = false;

    // https://github.com/pingcap/tidb/issues/16028
    public static boolean bug16028 = false;

    // https://github.com/pingcap/tidb/issues/16020
    public static boolean bug16020 = false;

    // https://github.com/pingcap/tidb/issues/15990
    public static boolean bug15990 = false;

    // https://github.com/pingcap/tidb/issues/15844
    public static boolean bug15844 = false;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/10
    public static boolean bug10 = false;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/14
    public static boolean bug14 = false;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/15
    public static boolean bug15 = false;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/16
    public static boolean bug16 = false;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/19
    public static boolean bug19 = false;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/48
    public static boolean bug48 = false;

    // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/50
    public static boolean bug50 = false;

    // https://github.com/citusdata/citus/issues/4079
    public static boolean bug4079 = false;

    // https://github.com/pingcap/tidb/issues/35677
    public static boolean bug35677 = true;

    // https://github.com/pingcap/tidb/issues/35522
    public static boolean bug35522 = true;

    // https://github.com/pingcap/tidb/issues/35652
    public static boolean bug35652 = true;

    // https://github.com/pingcap/tidb/issues/38295
    public static boolean bug38295 = true;

    private TiDBBugs() {
    }

}
