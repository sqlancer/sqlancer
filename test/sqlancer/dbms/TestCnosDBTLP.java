package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestCnosDBTLP {

    @Test
    public void testCnosDBTLP() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.CNOSDB_ENV));
        // Run with 0 queries as current implementation is resulting in database crashes
        assertEquals(0,
                Main.executeMain(new String[] { "--host", "127.0.0.1", "--port", "8902", "--username", "root",
                        "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-queries", "0", "cnosdb",
                        "--oracle", "QUERY_PARTITIONING" }));
    }

}
