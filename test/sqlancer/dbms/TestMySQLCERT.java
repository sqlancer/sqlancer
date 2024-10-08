package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestMySQLCERT {

    @Test
    public void testMySQL() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.MYSQL_ENV));
        // Run with 0 queries as there are false positives for every mutation
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--max-expression-depth", "1", "--num-threads", "1", "--num-queries", "0", "mysql", "--oracle",
                        "CERT" }));
    }

}
