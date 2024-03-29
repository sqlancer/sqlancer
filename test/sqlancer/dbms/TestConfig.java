package sqlancer.dbms;

public class TestConfig {
    public static final String NUM_QUERIES = "1000";
    public static final String SECONDS = "300";

    public static final String POSTGRES_ENV = "POSTGRES_AVAILABLE";

    public static boolean isEnvironmentTrue(String key) {
        String value = System.getenv(key);
        return value != null && value.equalsIgnoreCase("true");
    }
}
