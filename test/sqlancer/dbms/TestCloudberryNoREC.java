package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestCloudberryNoREC {

    @Test
    public void testCloudberryNoREC() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.CLOUDBERRY_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "--username", "gpadmin",
                        "cloudberry", "--connection-url", "postgresql://localhost:7000/test", "--test-collations",
                        "false", "--cloudberry-oracle", "NOREC" }));
    }

}
