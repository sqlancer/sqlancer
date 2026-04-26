package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestCnosDBNoREC {

    @Test
    public void testCnosDBNoREC() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.CNOSDB_ENV));
        // Run with 0 queries as current implementation is resulting in database crashes.
        // Run single-threaded with few iterations: CnosDB's storage layer cannot keep up
        // with concurrent CREATE/DROP DATABASE, returning EAGAIN ("Resource temporarily
        // unavailable (os error 11)") from the Tskv index storage.
        assertEquals(0,
                Main.executeMain(new String[] { "--host", "127.0.0.1", "--port", "8902", "--username", "root",
                        "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS, "--num-queries", "0",
                        "--num-threads", "1", "--num-tries", "5", "cnosdb", "--oracle", "NOREC" }));
    }

}
