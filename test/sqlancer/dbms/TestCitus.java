package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestCitus {

    @Test
    public void testCitus() {
        String citusAvailable = System.getenv("CITUS_AVAILABLE");
        boolean citusIsAvailable = citusAvailable != null && citusAvailable.equalsIgnoreCase("true");
        assumeTrue(citusIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "citus", "--connection-url",
                        "postgresql://localhost:9700/test", "--test-collations", "false" }));
    }

}
