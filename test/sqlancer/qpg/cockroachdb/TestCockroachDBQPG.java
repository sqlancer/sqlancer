package sqlancer.qpg.cockroachdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.dbms.TestConfig;

public class TestCockroachDBQPG {

    @Test
    public void testCockroachDBQPG() {
        String cockroachDB = System.getenv("COCKROACHDB_AVAILABLE");
        boolean cockroachDBIsAvailable = cockroachDB != null && cockroachDB.equalsIgnoreCase("true");
        assumeTrue(cockroachDBIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--qpg-enable", "true", "--num-queries", TestConfig.NUM_QUERIES,
                        "cockroachdb", "--oracle", "QUERY_PARTITIONING" }));
    }

}
