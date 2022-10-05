package sqlancer.dbms;

import org.junit.jupiter.api.Test;
import sqlancer.Main;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestYugabyte {

    String yugabyteAvailable = System.getenv("YUGABYTE_AVAILABLE");
    boolean yugabyteIsAvailable = yugabyteAvailable != null && yugabyteAvailable.equalsIgnoreCase("true");

    @Test
    public void testYCQL() {
        assumeTrue(yugabyteIsAvailable);
        assertEquals(0,
                Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--username",
                        "cassandra", "--password", "cassandra", "--num-threads", "1", "--num-queries",
                        TestConfig.NUM_QUERIES, "ycql"));
    }

    @Test
    public void testYSQL() {
        assumeTrue(yugabyteIsAvailable);
        assertEquals(0,
                Main.executeMain("--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--username",
                        "yugabyte", "--password", "yugabyte", "--num-threads", "1", "--num-queries",
                        TestConfig.NUM_QUERIES, "ysql"));
    }
}
