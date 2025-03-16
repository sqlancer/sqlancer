package sqlancer.clickhouse.gen;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseOptions;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.MainOptions;

class ClickHouseTableGeneratorTest {

    private ClickHouseProvider.ClickHouseGlobalState state;

    @BeforeEach
    void setUp() {
        // Initialize options
        MainOptions mainOptions = new MainOptions();
        ClickHouseOptions clickHouseOptions = new ClickHouseOptions();

        // Initialize global state
        state = new ClickHouseProvider.ClickHouseGlobalState();
        state.setMainOptions(mainOptions);
        state.setClickHouseOptions(clickHouseOptions);
        state.setDatabaseName("test_db");
        state.setRandomly(new Randomly());
    }

    @Test
    void testTableGenerationWithPrimaryKey() {
        // Generate multiple tables to increase chance of different scenarios
        for (int i = 0; i < 100; i++) {
            SQLQueryAdapter query = ClickHouseTableGenerator.createTableStatement("test_table_" + i, state);
            String sql = query.getQueryString();

            // if table has PRIMARY KEY, verfy it's also in ORDER BY
            if (sql.contains("PRIMARY KEY")) {
                // extract primary key columns
                int pkStart = sql.indexOf("PRIMARY KEY (") + "PRIMARY KEY (".length();
                int pkEnd = sql.indexOf(")", pkStart);
                String primaryKey = sql.substring(pkStart, pkEnd);

                // extract ORDER BY clause
                int orderByStart = sql.indexOf("ORDER BY (") + "ORDER BY (".length();
                int orderByEnd = sql.indexOf(")", orderByStart);
                String orderBy = sql.substring(orderByStart, orderByEnd);

                // Verify primary key is prefix of ORDER BY
                assertTrue(orderBy.startsWith(primaryKey),
                        "Primary key columns must be prefix of ORDER BY. PK: " + primaryKey + ", ORDER BY: " + orderBy);
            }

            // Verify no expressions in ORDER BY for MergeTree
            if (sql.contains("ENGINE = MergeTree")) {
                assertFalse(sql.contains("divide(") || sql.contains("plus(") || sql.contains("minus("),
                        "ORDER BY clause should not contain expressions");
            }

            // Verify tuple() is used correctly when no columns are selected
            if (sql.contains("ORDER BY tuple()")) {
                assertFalse(sql.contains("PRIMARY KEY"), "Table with tuple() ORDER BY should not have PRIMARY KEY");
            }
        }
    }
}
