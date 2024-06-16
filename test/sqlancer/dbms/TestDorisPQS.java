package sqlancer.dbms;

import org.junit.jupiter.api.Test;
import sqlancer.Main;
import sqlancer.Randomly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestDorisPQS {
    private final String host = "127.0.0.1";
    private final String port = "9030";
    private final String username = "sqlancer";
    private final String password = "sqlancer";

    @Test
    public void testdorisPQS() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.DORIS_ENV));
        assertEquals(0,
                Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "4",
                        "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "doris",
                        "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.ALPHANUMERIC),
                        "--username", username, "--password", password, "--host", host, "--port", port, "doris",
                        "--oracle", "PQS"));
    }
}
