package sqlancer.cockroachdb;

public final class CockroachDBBugs {

    // https://github.com/cockroachdb/cockroach/issues/46915
    public static boolean bug46915;

    // https://github.com/cockroachdb/cockroach/issues/45703
    public static boolean bug45703;

    // https://github.com/cockroachdb/cockroach/issues/44757
    public static boolean bug44757;

    // https://github.com/cockroachdb/cockroach/issues/83792
    public static boolean bug83792 = true;

    // https://github.com/cockroachdb/cockroach/issues/83874
    public static boolean bug83874 = true;

    // https://github.com/cockroachdb/cockroach/issues/83973
    public static boolean bug83973;

    // https://github.com/cockroachdb/cockroach/issues/83976
    public static boolean bug83976;

    // The following bug is closed, but leave it enabled until
    // the underlying interval issue is resolved.
    // https://github.com/cockroachdb/cockroach/issues/84078
    // https://github.com/cockroachdb/cockroach/issues/84154
    public static boolean bug84154 = true;

    // https://github.com/cockroachdb/cockroach/issues/85356
    public static boolean bug85356;

    // https://github.com/cockroachdb/cockroach/issues/85371
    public static boolean bug85371;

    // https://github.com/cockroachdb/cockroach/issues/85389
    public static boolean bug85389;

    // https://github.com/cockroachdb/cockroach/issues/85390
    public static boolean bug85390;

    // https://github.com/cockroachdb/cockroach/issues/85393
    public static boolean bug85393;

    // https://github.com/cockroachdb/cockroach/issues/85394
    public static boolean bug85394 = true;

    // https://github.com/cockroachdb/cockroach/issues/85441
    public static boolean bug85441;

    // https://github.com/cockroachdb/cockroach/issues/85499
    public static boolean bug85499;

    // https://github.com/cockroachdb/cockroach/issues/88037
    public static boolean bug88037;

    private CockroachDBBugs() {
    }

}
