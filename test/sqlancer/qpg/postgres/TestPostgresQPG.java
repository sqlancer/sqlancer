package sqlancer.qpg.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.dbms.TestConfig;

public class TestPostgresQPG {

    @Test
    public void testPostgresQPG() {
        String postgres = System.getenv("POSTGRES_AVAILABLE");
        boolean postgresIsAvailable = postgres != null && postgres.equalsIgnoreCase("true");
        assumeTrue(postgresIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--qpg-enable", "true", "--num-queries", TestConfig.NUM_QUERIES,
                        "--username", "postgres", "postgres", "--oracle", "NOREC" }));
    }
}
