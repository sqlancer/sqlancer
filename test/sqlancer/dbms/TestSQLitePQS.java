package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestSQLitePQS {

    @Test
    public void testSqlitePQS() {
        // run with one thread due to multithreading issues, see https://github.com/sqlancer/sqlancer/pull/45
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-threads", "1", "--num-queries", "1000", "--random-string-generation", "ALPHANUMERIC_SPECIALCHAR",
                "--database-prefix", "pqsdb" /* Workaround for connections not being closed */, "sqlite3", "--oracle",
                "PQS", "--test-fts", "false", "--test-rtree", "false", "--test-check-constraints", "false",
                "--test-in-operator", "false" }));
    }

}
