package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestMaterialize {

    String materializeAvailable = System.getenv("MATERIALIZE_AVAILABLE");
    boolean materializeIsAvailable = materializeAvailable != null && materializeAvailable.equalsIgnoreCase("true");

    @Test
    public void testMaterialize() {
        assumeTrue(materializeIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "--host", "localhost",
                        "--username", "materialize", "--port", "6875", "materialize", "--set-max-tables-mvs", "true" }));
    }

    @Test
    public void testMaterializePQS() {
        assumeTrue(materializeIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "--random-string-generation",
                        "ALPHANUMERIC_SPECIALCHAR", "--host", "localhost", "--username", "materialize", "--port", "6875",
                        "materialize", "--oracle", "pqs", "--set-max-tables-mvs", "true" }));
    }

}
