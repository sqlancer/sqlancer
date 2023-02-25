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
                        "5", "--username", "default", "--password", "", "--database-prefix", "T1_", "clickhouse",
                        "--oracle", "TLPWhere", "--oracle", "TLPGroupBy"));
    }

    @Test
    public void testClickHouseTLPWhere() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES, "--num-threads",
                        "5", "--username", "default", "--password", "", "--database-prefix", "T2_", "clickhouse",
                        "--oracle", "TLPWhere"));
    }

    @Test
    public void testClickHouseTLPHaving() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "1", "--username", "default",
                        "--password", "", "--database-prefix", "T3_", "clickhouse", "--oracle", "TLPHaving"));
    }

    @Test
    public void testClickHouseTLPGroupBy() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "5", "--username", "default",
                        "--password", "", "--database-prefix", "T4_", "clickhouse", "--oracle", "TLPGroupBy"));
    }

    @Test
    public void testClickHouseTLPDistinct() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "5", "--username", "default",
                        "--password", "", "--database-prefix", "T5_", "clickhouse", "--oracle", "TLPDistinct"));
    }

    @Test
    public void testClickHouseTLPAggregate() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "5", "--username", "default",
                        "--password", "", "--database-prefix", "T6_", "clickhouse", "--oracle", "TLPAggregate"));
    }

    @Test
    public void testClickHouseNoREC() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "1", "--username", "default",
                        "--password", "", "--database-prefix", "T7_", "clickhouse", "--oracle", "NoREC"));
    }

    @Test
    public void testClickHouseTLPWhereGroupByWithJoins() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES, "--num-threads",
                        "5", "--username", "default", "--password", "", "--database-prefix", "T8_", "clickhouse",
                        "--oracle", "TLPWhere", "--oracle", "TLPGroupBy"));
    }

    @Test
    public void testClickHouseTLPWhereWithJoins() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--timeout-seconds", "60", "--num-queries", TestConfig.NUM_QUERIES, "--num-threads",
                        "5", "--username", "default", "--password", "", "--database-prefix", "T9_", "clickhouse",
                        "--oracle", "TLPWhere"));
    }

    @Test
    public void testClickHouseTLPHavingWithJoins() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "1", "--username", "default",
                        "--password", "", "--database-prefix", "T10_", "clickhouse", "--oracle", "TLPHaving"));
    }

    @Test
    public void testClickHouseTLPGroupByWithJoins() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "5", "--username", "default",
                        "--password", "", "--database-prefix", "T11_", "clickhouse", "--oracle", "TLPGroupBy"));
    }

    @Test
    public void testClickHouseTLPDistinctWithJoins() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "5", "--username", "default",
                        "--password", "", "--database-prefix", "T12_", "clickhouse", "--oracle", "TLPDistinct"));
    }

    @Test
    public void testClickHouseTLPAggregateWithJoins() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "5", "--username", "default",
                        "--password", "", "--database-prefix", "T13_", "clickhouse", "--oracle", "TLPAggregate"));
    }

    @Test
    public void testClickHouseNoRECWithJoins() {
        String clickHouseAvailable = System.getenv("CLICKHOUSE_AVAILABLE");
        boolean clickHouseIsAvailable = clickHouseAvailable != null && clickHouseAvailable.equalsIgnoreCase("true");
        assumeTrue(clickHouseIsAvailable);
        assertEquals(0,
                Main.executeMain("--log-each-select", "true", "--print-failed", "false", "--timeout-seconds", "60",
                        "--num-queries", TestConfig.NUM_QUERIES, "--num-threads", "1", "--username", "default",
                        "--password", "", "--database-prefix", "T14_", "clickhouse", "--oracle", "NoREC"));
    }

}
