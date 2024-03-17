package sqlancer.tidb;

// do not make the fields final to avoid warnings
public final class TiDBBugs {
    // https://github.com/pingcap/tidb/issues/35677
    public static boolean bug35677 = true;

    // https://github.com/pingcap/tidb/issues/35522
    public static boolean bug35522 = true;

    // https://github.com/pingcap/tidb/issues/35652
    public static boolean bug35652 = true;

    // https://github.com/pingcap/tidb/issues/38295
    public static boolean bug38295 = true;

    // https://github.com/pingcap/tidb/issues/44747
    public static boolean bug44747 = true;

    // https://github.com/pingcap/tidb/issues/46556
    public static boolean bug46556 = true;

    // https://github.com/pingcap/tidb/issues/46591
    public static boolean bug46591 = true;

    // https://github.com/pingcap/tidb/issues/46598
    public static boolean bug46598 = true;

    // https://github.com/pingcap/tidb/issues/47346
    public static boolean bug47346 = true;

    // https://github.com/pingcap/tidb/issues/47348
    public static boolean bug47348 = true;

    private TiDBBugs() {
    }

}
