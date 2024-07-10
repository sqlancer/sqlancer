package sqlancer.dqp.mariadb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.dbms.TestConfig;

public class TestMariaDBDQP {

    @Test
    public void testMariaDBDQPMethod() {
        String mariadb = System.getenv("MARIADB_AVAILABLE");
        boolean mariadbIsAvailable = mariadb != null && mariadb.equalsIgnoreCase("true");
        assumeTrue(mariadbIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-threads", "1", "--num-queries", TestConfig.NUM_QUERIES, "mariadb", "--oracle", "DQP" }));
    }

}
