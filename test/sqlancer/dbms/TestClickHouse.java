package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sqlancer.Main;
import sqlancer.dbms.TestConfig;

public class TestClickHouse {

    @Test
    public void testClickHouseTLPWhereHaving() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        Assertions.assertEquals(0,
                Main.executeMain(new String[] { "--timeout-seconds", TestConfig.SECONDS, "--num-queries",
                        TestConfig.NUM_QUERIES, "--num-threads", "50", "clickhouse", "--oracle", "TLPWhere", "--oracle",
                        "TLPHaving" }));
    }

    @Test
    public void testClickHouseTLPWhere() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        Assertions.assertEquals(0,
                Main.executeMain(new String[] { "--timeout-seconds", TestConfig.SECONDS, "--num-queries",
                        TestConfig.NUM_QUERIES, "--num-threads", "50", "clickhouse", "--oracle", "TLPWhere" }));
    }

    @Test
    public void testClickHouseTLPHaving() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", TestConfig.SECONDS, "--num-queries",
                TestConfig.NUM_QUERIES, "--num-threads", "30", "clickhouse", "--oracle", "TLPHaving" }));
    }

    @Test
    public void testClickHouseTLPGroupBy() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--timeout-seconds", TestConfig.SECONDS, "--num-queries",
                TestConfig.NUM_QUERIES, "--num-threads", "30", "clickhouse", "--oracle", "TLPGroupBy" }));
    }
}
