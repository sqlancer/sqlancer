package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestMySQLPQS {

    @Test
    public void testPQS() {
//        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.MYSQL_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--random-string-generation", "ALPHANUMERIC", "--database-prefix",
                        "pqsdb" /* Workaround for connections not being closed */, "--num-queries",
                        TestConfig.NUM_QUERIES,
                        "--host", "localhost", "--port", "3306", "--username", "root", "--password", "password",
                        "mysql", "--oracle", "PQS" }));
    }

}


