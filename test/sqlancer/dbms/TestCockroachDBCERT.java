package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestCockroachDBCERT {

    @Test
    public void testCockroachDBCERT() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.COCKROACHDB_ENV));
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "cockroachdb", "--oracle", "CERT" }));
    }

}
