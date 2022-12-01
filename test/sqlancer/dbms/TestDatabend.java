package sqlancer.dbms;

import org.junit.jupiter.api.Test;
import sqlancer.Main;
import sqlancer.Randomly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestDatabend {

    @Test
    public void testDatabendNoREC() {
        String databendAvailable = System.getenv("DATABEND_AVAILABLE");
        boolean databendIsAvailable = databendAvailable != null && databendAvailable.equalsIgnoreCase("true");
        assumeTrue(databendIsAvailable);
        assertEquals(0,
                Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
                        "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
                        "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.ALPHANUMERIC),
                        "--host", "127.0.0.1", "--port", "3307", "databend", "--oracle", "NOREC"));
    }

    @Test
    public void testDatabendTLPQueryPartitioning() {
        String databendAvailable = System.getenv("DATABEND_AVAILABLE");
        boolean databendIsAvailable = databendAvailable != null && databendAvailable.equalsIgnoreCase("true");
        assumeTrue(databendIsAvailable);
        assertEquals(0,
                Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
                        "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
                        "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.ALPHANUMERIC),
                        "--host", "127.0.0.1", "--port", "3307", "databend", "--oracle", "QUERY_PARTITIONING"));
    }

    // @Test
    // public void testDatabendTLPWhere() {
    // String databendAvailable = System.getenv("DATABEND_AVAILABLE");
    // boolean databendIsAvailable = databendAvailable != null && databendAvailable.equalsIgnoreCase("true");
    // assumeTrue(databendIsAvailable);
    // assertEquals(0,
    // Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
    // "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
    // "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.NUMERIC),
    // "--host", "127.0.0.1", "--port", "3307", "databend", "--oracle", "WHERE"));
    // }
    //
    // @Test
    // public void testDatabendTLPGroupBy() {
    // String databendAvailable = System.getenv("DATABEND_AVAILABLE");
    // boolean databendIsAvailable = databendAvailable != null && databendAvailable.equalsIgnoreCase("true");
    // assumeTrue(databendIsAvailable);
    // assertEquals(0,
    // Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
    // "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
    // "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.NUMERIC),
    // "--host", "127.0.0.1", "--port", "3307", "databend", "--oracle", "GROUP_BY"));
    // }
    //
    // @Test
    // public void testDatabendTLPHaving() {
    // String databendAvailable = System.getenv("DATABEND_AVAILABLE");
    // boolean databendIsAvailable = databendAvailable != null && databendAvailable.equalsIgnoreCase("true");
    // assumeTrue(databendIsAvailable);
    // assertEquals(0,
    // Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
    // "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
    // "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.NUMERIC),
    // "--host", "127.0.0.1", "--port", "3307", "databend", "--oracle", "HAVING"));
    // }
    //
    // @Test
    // public void testDatabendTLPDistinct() {
    // String databendAvailable = System.getenv("DATABEND_AVAILABLE");
    // boolean databendIsAvailable = databendAvailable != null && databendAvailable.equalsIgnoreCase("true");
    // assumeTrue(databendIsAvailable);
    // assertEquals(0,
    // Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
    // "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
    // "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.NUMERIC),
    // "--host", "127.0.0.1", "--port", "3307", "databend", "--oracle", "DISTINCT"));
    // }
    //
    // @Test
    // public void testDatabendTLPAggregate() {
    // String databendAvailable = System.getenv("DATABEND_AVAILABLE");
    // boolean databendIsAvailable = databendAvailable != null && databendAvailable.equalsIgnoreCase("true");
    // assumeTrue(databendIsAvailable);
    // assertEquals(0,
    // Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
    // "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
    // "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.NUMERIC),
    // "--host", "127.0.0.1", "--port", "3307", "databend", "--oracle", "AGGREGATE"));
    // }

    // @Test
    // void testConnection() {
    // assertEquals(0,
    // Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
    // "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
    // "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.NUMERIC),
    // "--host", "127.0.0.1", "--port", "3307", "--username", "user1", "--password", "1234",
    // "databend", "--oracle", "HAVING" }));
    // }

}
