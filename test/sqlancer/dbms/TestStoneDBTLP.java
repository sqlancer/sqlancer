package sqlancer.dbms;

import org.junit.jupiter.api.Test;
import sqlancer.Main;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestStoneDBTLP {

    String stoneDBAvailable = System.getenv("STONEDB_AVAILABLE");
    boolean stoneDBIsAvailable = stoneDBAvailable != null && stoneDBAvailable.equalsIgnoreCase("true");

    @Test
    public void testStoneDB() {
        assumeTrue(stoneDBIsAvailable);
        assertEquals(0, Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads",
                "1", "--num-queries", TestConfig.NUM_QUERIES, "stonedb", "--oracle", "QUERY_PARTITIONING"));
    }

}
