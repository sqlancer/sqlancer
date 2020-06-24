package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestMain {

    private static final String NUM_QUERIES = "1000";
    private static final String SECONDS = "300";

    @Test
    public void testDuckDB() {
        // run with one thread due to multithreading issues, see https://github.com/sqlancer/sqlancer/pull/45
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", SECONDS, "--num-threads", "1",
                "--num-queries", NUM_QUERIES, "duckdb", "--oracle", "NoREC" }));
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", SECONDS, "--num-threads", "1",
                "--num-queries", NUM_QUERIES, "duckdb", "--oracle", "QUERY_PARTITIONING" }));
    }

}