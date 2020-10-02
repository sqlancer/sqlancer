package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestMySQLTLP {

    String mysqlAvailable = System.getenv("MYSQL_AVAILABLE");
    boolean mysqlIsAvailable = mysqlAvailable != null && mysqlAvailable.equalsIgnoreCase("true");

    @Test
    public void testMySQL() {
        assumeTrue(mysqlIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--max-expression-depth", "1", "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES,
                        "mysql", "--oracle", "TLP_WHERE" }));
    }

}
