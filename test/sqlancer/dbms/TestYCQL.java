package sqlancer.dbms;

import org.junit.jupiter.api.Test;
import sqlancer.Main;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestYCQL {
    @Test
    public void testYCQL() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.YUGABYTE_ENV));
        assertEquals(0,
                Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--username",
                        "cassandra", "--password", "cassandra", "--num-threads", "1", "--num-queries",
                        TestConfig.NUM_QUERIES, "ycql"));
    }
}
