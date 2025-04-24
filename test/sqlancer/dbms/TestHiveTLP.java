package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestHiveTLP {

    @Test
    public void testHiveTLPWhere() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.HIVE_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--canonicalize-sql-strings", "false", "--random-seed", "0",
                        "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "1", "--num-queries",
                        TestConfig.NUM_QUERIES, "hive", "--oracle", "TLPWhere" }));
    }
}