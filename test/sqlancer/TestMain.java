package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestMain {

    private static final String NUM_QUERIES = "1000";
    private static final String SECONDS = "30";

    @Test
    public void testSQLite() {
        // do not check the result, since we still find bugs in the SQLite3 version included in the JDBC driver
        Main.executeMain(new String[] { "--timeout-seconds", SECONDS, "--num_queries", NUM_QUERIES, "sqlite3",
                "--oracle", "NoREC" });
    }

    @Test
    public void testDuckDB() {
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", SECONDS, "--num_queries", NUM_QUERIES,
                "duckdb", "--oracle", "NoREC" }));
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", SECONDS, "--num_queries", NUM_QUERIES,
                "duckdb", "--oracle", "QUERY_PARTITIONING" }));
    }

}
