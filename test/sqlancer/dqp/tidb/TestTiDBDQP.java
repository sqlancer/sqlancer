package sqlancer.dqp.tidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.dbms.TestConfig;

public class TestTiDBDQP {

    @Test
    public void testTiDBQPG() {
        String tiDB = System.getenv("TIDB_AVAILABLE");
        boolean tiDBIsAvailable = tiDB != null && tiDB.equalsIgnoreCase("true");
        assumeTrue(tiDBIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-threads", "1", "--num-queries", TestConfig.NUM_QUERIES, "tidb", "--oracle", "DQP" }));
    }

}
