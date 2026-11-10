package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestVoltDB {

    // needs a VoltDB server on localhost:21212 with security disabled
    @Test
    public void testVoltDBSmoke() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.VOLTDB_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "2", "--num-queries", "5", "--username", "", "--password", "", "voltdb",
                        "--oracle", "SMOKE" }));
    }

    @Test
    public void testVoltDBNoREC() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.VOLTDB_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "2", "--num-queries", "100", "--username", "", "--password", "", "voltdb",
                        "--oracle", "NOREC" }));
    }

    @Test
    public void testVoltDBTLPWhere() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.VOLTDB_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "2", "--num-queries", "100", "--username", "", "--password", "", "voltdb",
                        "--oracle", "TLP_WHERE" }));
    }

    @Test
    public void testVoltDBTLPDistinctHavingAggregate() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.VOLTDB_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "2", "--num-queries", "100", "--username", "", "--password", "", "voltdb",
                        "--oracle", "TLP_DISTINCT,TLP_HAVING,TLP_AGGREGATE" }));
    }

    @Test
    public void testVoltDBPQS() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.VOLTDB_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "2", "--num-queries", "100", "--username", "", "--password", "", "voltdb",
                        "--oracle", "PQS" }));
    }

}
