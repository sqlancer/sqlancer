package sqlancer.databend;

import org.junit.jupiter.api.Test;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.dbms.TestConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDatabendConnection {
    @Test
    void testConnection() {
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "--database-prefix", "databend",
                        "--random-string-generation", String.valueOf(Randomly.StringGenerationStrategy.NUMERIC),
                        "--host", "192.168.191.151", "--port", "3307", "--username", "user1", "--password", "1234",
                        "databend", "--oracle", "HAVING" }));
    }
}
