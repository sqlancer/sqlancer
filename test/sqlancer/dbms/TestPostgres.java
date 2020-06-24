package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestPostgres {

    @Test
    public void testPostgres() {
        String postgresAvailable = System.getenv("POSTGRES_AVAILABLE");
        boolean postgresIsAvailable = postgresAvailable != null && postgresAvailable.equalsIgnoreCase("true");
        assumeTrue(postgresIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
                "--num-queries", TestConfig.NUM_QUERIES, "postgres", "--test-collations", "false" }));
    }

}
