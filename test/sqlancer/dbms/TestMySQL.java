package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestMySQL {

    String mysqlAvailable = System.getenv("MYSQL_AVAILABLE");
    boolean mysqlIsAvailable = mysqlAvailable != null && mysqlAvailable.equalsIgnoreCase("true");

    @Test
    public void testPQS() {
        assumeTrue(mysqlIsAvailable);
        assertEquals(0,
                /*
                 * While the MySQL generation supports ALPHANUMERIC as string generation strategy, the Travis CI gate
                 * seems to fail due to special characters that are not supposed to be generated, and which cannot be
                 * reproduced locally.
                 */
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--random-string-generation", "NUMERIC", "--database-prefix",
                        "pqsdb" /* Workaround for connections not being closed */, "--num-queries",
                        TestConfig.NUM_QUERIES, "mysql", "--oracle", "PQS" }));
    }

    @Test
    public void testMySQL() {
        assumeTrue(mysqlIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--max-expression-depth", "1", "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES,
                        "mysql", "--oracle", "TLP_WHERE" }));
    }

}
