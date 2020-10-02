package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestPostgres {

    String postgresAvailable = System.getenv("POSTGRES_AVAILABLE");
    boolean postgresIsAvailable = postgresAvailable != null && postgresAvailable.equalsIgnoreCase("true");

    @Test
    public void testPostgres() {
        assumeTrue(postgresIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "postgres", "--test-collations",
                        "false" }));
    }

    @Test
    public void testPQS() {
        assumeTrue(postgresIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "--random-string-generation",
                        "ALPHANUMERIC_SPECIALCHAR", "--database-prefix",
                        "pqsdb" /* Workaround for connections not being closed */, "postgres", "--test-collations",
                        "false", "--oracle", "pqs" }));
    }

}
