package sqlancer.stonedb;

public final class StoneDBBugs {
    // https://github.com/stoneatom/stonedb/issues/1933
    public static boolean bug1933 = true;
    // CREATE TABLE t0(c0 INT) ;
    // SELECT * FROM t0 WHERE ((t0.c0)XOR((t0.c0)));
    // ERROR 6 (HY000): The query includes syntax that is not supported by the storage engine. Either restructure the
    // query with supported syntax, or enable the MySQL core::Query Path in config file to execute the query with
    // reduced performance.
    public static boolean bugNotReportedXOR = true;

    private StoneDBBugs() {
    }
}
