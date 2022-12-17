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

    private TiDBBugs() {
    }

}
