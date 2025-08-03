package sqlancer.dbms;

import org.junit.jupiter.api.Test;
import sqlancer.Main;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestMySQLDQE {

    @Test
    public void testMySQL() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.MYSQL_ENV));
        // Run with 0 queries as there are false positives for every mutation
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--max-expression-depth", "1", "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES,
                        "mysql", "--oracle", "DQE" }));
    }

}
