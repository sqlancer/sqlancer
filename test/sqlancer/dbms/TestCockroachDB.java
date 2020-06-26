package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestCockroachDB {

    @Test
    public void testMySQL() {
        String cockroachDB = System.getenv("COCKROACHDB_AVAILABLE");
        boolean cockroachDBIsAvailable = cockroachDB != null && cockroachDB.equalsIgnoreCase("true");
        assumeTrue(cockroachDBIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-queries", TestConfig.NUM_QUERIES, "cockroachdb" }));
    }

}
