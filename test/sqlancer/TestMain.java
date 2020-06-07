package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestMain {

    @Test
    public void testSQLite() {
        // do not check the result, since we still find bugs in the SQLite3 version included in the JDBC driver
        Main.executeMain(new String[] { "--timeout-seconds", "30", "sqlite3", "--oracle", "NoREC" });
    }

    @Test
    public void testDuckDB() {
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", "30", "duckdb", "--oracle", "NoREC" }));
    }

}
