package sqlancer;

import org.junit.jupiter.api.Test;

public class TestMain {

    @Test
    public void testSQLite() {
        Main.main(new String[] { "--timeout-seconds", "30", "sqlite3", "--oracle", "NoREC" });
    }

    @Test
    public void testDuckDB() {
        Main.main(new String[] { "--timeout-seconds", "30", "duckdb", "--oracle", "NoREC" });
    }

}
