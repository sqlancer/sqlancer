package sqlancer.cloudberry;

// do not make the fields final to avoid warnings
public final class CloudberryBugs {

    // https://github.com/apache/cloudberry/issues/2126
    public static boolean bug2126 = true;

    private CloudberryBugs() {
    }

}
