package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestClickHouse {

    @Test
    public void testClickHouseTLPWhereGroupBy() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES, "--num-threads",
                        "5", "--username", "default", "--password", "", "clickhouse", "--oracle", "TLPWhere",
                        "--oracle", "TLPGroupBy"));
    }

    @Test
    public void testClickHouseTLPWhere() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0, Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES,
                "--num-threads", "5", "--username", "default", "--password", "", "clickhouse", "--oracle", "TLPWhere"));
    }

    @Test
    public void testClickHouseTLPHaving() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES, "--num-threads",
                        "5", "--username", "default", "--password", "", "clickhouse", "--oracle", "TLPHaving"));
    }

    @Test
    public void testClickHouseTLPGroupBy() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES, "--num-threads",
                        "5", "--username", "default", "--password", "", "clickhouse", "--oracle", "TLPGroupBy"));
    }

    @Test
    public void testClickHouseTLPDistinct() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES, "--num-threads",
                        "5", "--username", "default", "--password", "", "clickhouse", "--oracle", "TLPDistinct"));
    }

    @Test
    public void testClickHouseTLPAggregate() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES, "--num-threads",
                        "5", "--username", "default", "--password", "", "clickhouse", "--oracle", "TLPAggregate"));
    }

}
