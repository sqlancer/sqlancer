package sqlancer.presto;

public final class PrestoBugs {

    // https://github.com/prestodb/presto/issues/23324
    public static boolean bug23324 = true;

    // https://github.com/prestodb/presto/issues/23613
    public static boolean bug23613 = true;

    // https://github.com/prestodb/presto/issues/27608
    public static boolean bugVerifyError = true;

    // https://github.com/prestodb/presto/issues/27609
    public static boolean bugCompilerFailed = true;

    private PrestoBugs() {
    }

}
