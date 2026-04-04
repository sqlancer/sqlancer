package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestSparkTLP {

    @Test
    public void testSparkTLPWhere() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.SPARK_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--canonicalize-sql-strings", "false", "--random-seed", "0",
                        "--timeout-seconds", TestConfig.SECONDS, "--num-threads", "1", "--num-queries",
                        TestConfig.NUM_QUERIES, "spark", "--oracle", "TLPWhere" }));
    }
}