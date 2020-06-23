package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestMain {

    private static final String NUM_QUERIES = "1000";
    private static final String SECONDS = "300";

    @Test
    public void testDuckDB() {
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", SECONDS, "--num-queries", NUM_QUERIES,
                "duckdb", "--oracle", "NoREC" }));
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", SECONDS, "--num-queries", NUM_QUERIES,
                "duckdb", "--oracle", "QUERY_PARTITIONING" }));
    }

}