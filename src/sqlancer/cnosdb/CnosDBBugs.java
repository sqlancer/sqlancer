package sqlancer.cnosdb;

public final class CnosDBBugs {

    // https://github.com/cnosdb/cnosdb/issues/786
    public static boolean bug786;

    // https://github.com/apache/arrow-rs/issues/3547
    public static boolean bug3547;

    private CnosDBBugs() {
    }
}
