package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestYSQLNoREC {
    @Test
    public void testYSQLNoREC() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.YUGABYTE_ENV));
        assertEquals(0,
                Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--username",
                        "yugabyte", "--password", "yugabyte", "--num-threads", "1", "--num-queries",
                        TestConfig.NUM_QUERIES, "ysql", "--oracle", "NOREC"));
    }
}
