package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestPrestoNoREC {
    @Test
    public void testPrestoNoREC() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.PRESTO_ENV));
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "presto", "--oracle", "NOREC" }));
    }
}
