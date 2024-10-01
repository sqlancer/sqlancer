package sqlancer.postgres;

// do not make the fields final to avoid warnings
public final class PostgresBugs {
    public static boolean bug18643 = true;

    private PostgresBugs() {
    }

}
