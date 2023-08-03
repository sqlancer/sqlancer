package sqlancer.stonedb;

public final class StoneDBBugs {
    // https://github.com/stoneatom/stonedb/issues/1933
    public static boolean bug1933 = true;
    // https://github.com/stoneatom/stonedb/issues/1942
    public static boolean bug1942 = true;
    // https://github.com/stoneatom/stonedb/issues/1953
    public static boolean bug1953 = true;
    // CREATE TABLE t0(c0 INT);
    // INSERT IGNORE INTO t0(c0) VALUE (DEFAULT);
    // SELECT t0.c0 FROM t0 WHERE 0.4; -- expect 1 but got 0
    public static boolean bugNotReported1 = true;
    // DELETE LOW_PRIORITY FROM t0;
    // will result into crash
    public static boolean bugNotReported2 = true;

    private StoneDBBugs() {
    }
}
