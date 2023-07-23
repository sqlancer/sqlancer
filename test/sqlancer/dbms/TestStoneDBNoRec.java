package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestStoneDBNoRec {

    String stoneDBAvailable = System.getenv("STONEDB_AVAILABLE");
    boolean stoneDBIsAvailable = stoneDBAvailable != null && stoneDBAvailable.equalsIgnoreCase("true");

    @Test
    public void testStoneDB() {
        assumeTrue(stoneDBIsAvailable);
        assertEquals(0, Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-threads",
                "1", "--num-queries", TestConfig.NUM_QUERIES, "stonedb", "--oracle", "NoREC"));
    }

}
