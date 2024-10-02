package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestTiDBCERT {

    @Test
    public void testCERT() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.TIDB_ENV));
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-queries", "4", "tidb", "--oracle", "CERT" }));
    }

}
