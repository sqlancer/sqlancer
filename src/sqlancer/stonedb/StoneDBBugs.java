package sqlancer.stonedb;

public final class StoneDBBugs {
    // https://github.com/stoneatom/stonedb/issues/1933
    public static boolean bug1933 = true;
    // https://github.com/stoneatom/stonedb/issues/1942
    public static boolean bug1942 = true;
    // https://github.com/stoneatom/stonedb/issues/1953
    public static boolean bug1953 = true;
    // https://github.com/stoneatom/stonedb/issues/1945
    public static boolean bug1945 = true;
    // CREATE TABLE t0(c0 INT);
    // INSERT IGNORE INTO t0(c0) VALUE (DEFAULT);
    // SELECT t0.c0 FROM t0 WHERE 0.4; -- expect 1 but got 0
    public static boolean bugNotReported1 = true;
    // DELETE statements will result into crash, for example
    // DELETE LOW_PRIORITY FROM t0;
    // DELETE QUICK IGNORE FROM t0 WHERE -1370759901;
    public static boolean bugNotReported2 = true;
    // CREATE TABLE t0(c0 INT) ;
    // INSERT INTO t0(c0) VALUES (DEFAULT);
    // SELECT t0.c0 FROM t0 WHERE (('OC')>=(((t0.c0) IS NULL))); -- expected empty set but got 1 row
    public static boolean bugNotReported3 = true;
    // CREATE TABLE t0(c0 CHAR PRIMARY KEY);
    // INSERT IGNORE INTO t0(c0) VALUE (DEFAULT);
    // SELECT t0.c0 FROM t0 WHERE (t0.c0 BETWEEN (0) AND t0.c0); -- 1
    public static boolean bugNotReported4 = true;
    // CREATE TABLE t0(c0 INT);
    // INSERT INTO t0(c0) VALUE (DEFAULT);
    // SELECT * FROM t0 WHERE (('00')>((('')AND(t0.c0)))); -- expected empty set but got 1 row
    public static boolean bugNotReported5 = true;
    // CREATE TABLE t0(c0 INT) ;
    // INSERT INTO t0(c0) VALUE (100);
    // SELECT * FROM t0 WHERE (('Xg')>(t0.c0)); -- expected empty set but got 1 row
    // SELECT (('Xg')>(t0.c0)) FROM t0;
    public static boolean bugNotReported6 = true;
    // CREATE TABLE t0(c0 VARCHAR(1000));
    // INSERT INTO t0(c0) VALUE ('GX9vartD');
    // INSERT INTO t0(c0) VALUE ('16');
    // SELECT t0.c0 FROM t0 WHERE t0.c0; -- expected 1 row but got 2 rows
    public static boolean bugNotReported7 = true;

    private StoneDBBugs() {
    }
}
