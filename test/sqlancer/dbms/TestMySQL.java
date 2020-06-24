package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestMySQL {

    @Test
    public void testMySQL() {
        String mysqlAvailable = System.getenv("MYSQL_AVAILABLE");
        boolean mysqlIsAvailable = mysqlAvailable != null && mysqlAvailable.equalsIgnoreCase("true");
        assumeTrue(mysqlIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--timeout-seconds", TestConfig.SECONDS, "--max-expression-depth", "1",
                        "--num-threads", "1", "--num-queries", TestConfig.NUM_QUERIES, "mysql" }));
    }

}
