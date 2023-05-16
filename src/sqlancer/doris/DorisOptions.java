package sqlancer.doris;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.doris.DorisOptions.DorisOracleFactory;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.oracle.DorisNoRECOracle;
import sqlancer.doris.oracle.DorisPivotedQuerySynthesisOracle;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningAggregateTester;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningDistinctTester;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningGroupByTester;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningHavingTester;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningWhereTester;

@Parameters(commandDescription = "Apache Doris (default port: " + DorisOptions.DEFAULT_PORT + ", default host: "
        + DorisOptions.DEFAULT_HOST + ")")
public class DorisOptions implements DBMSSpecificOptions<DorisOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9030;

    @Parameter(names = { "--max-num-tables" }, description = "The maximum number of tables/views that can be created")
    public int maxNumTables = 10;

    @Parameter(names = { "--max-num-indexes" }, description = "The maximum number of indexes that can be created")
    public int maxNumIndexes = 20;

    @Parameter(names = "--test-default-values", description = "Allow generating DEFAULT values in tables", arity = 1)
    public boolean testDefaultValues = true;

    @Parameter(names = "--test-not-null", description = "Allow generating NOT NULL constraints in tables", arity = 1)
    public boolean testNotNullConstraints = true;

    @Parameter(names = "--test-functions", description = "Allow generating functions in expressions", arity = 1)
    public boolean testFunctions;

    @Parameter(names = "--test-casts", description = "Allow generating casts in expressions", arity = 1)
    public boolean testCasts = true;

    @Parameter(names = "--test-between", description = "Allow generating the BETWEEN operator in expressions", arity = 1)
    public boolean testBetween = true;

    @Parameter(names = "--test-in", description = "Allow generating the IN operator in expressions", arity = 1)
    public boolean testIn = true;

    @Parameter(names = "--test-case", description = "Allow generating the CASE operator in expressions", arity = 1)
    public boolean testCase = true;

    @Parameter(names = "--test-binary-logicals", description = "Allow generating AND and OR in expressions", arity = 1)
    public boolean testBinaryLogicals = true;

    @Parameter(names = "--test-int-constants", description = "Allow generating INTEGER constants", arity = 1)
    public boolean testIntConstants = true;

    @Parameter(names = "--test-float-constants", description = "Allow generating floating-point constants", arity = 1)
    public boolean testFloatConstants = true;

    @Parameter(names = "--test-decimal-constants", description = "Allow generating DECIMAL constants", arity = 1)
    public boolean testDecimalConstants = true;

    @Parameter(names = "--test-date-constants", description = "Allow generating DATE constants", arity = 1)
    public boolean testDateConstants = true;

    @Parameter(names = "--test-datetime-constants", description = "Allow generating DATETIME constants", arity = 1)
    public boolean testDateTimeConstants = true;

    @Parameter(names = "--test-varchar-constants", description = "Allow generating VARCHAR constants", arity = 1)
    public boolean testStringConstants = true;

    @Parameter(names = "--test-boolean-constants", description = "Allow generating boolean constants", arity = 1)
    public boolean testBooleanConstants = true;

    @Parameter(names = "--test-binary-comparisons", description = "Allow generating binary comparison operators (e.g., >= or LIKE)", arity = 1)
    public boolean testBinaryComparisons = true;

    @Parameter(names = "--max-num-deletes", description = "The maximum number of DELETE statements that are issued for a database", arity = 1)
    public int maxNumDeletes = 1;

    @Parameter(names = "--max-num-updates", description = "The maximum number of UPDATE statements that are issued for a database", arity = 1)
    public int maxNumUpdates;

    @Parameter(names = "--max-num-table-alters", description = "The maximum number of ALTER TABLE statements that are issued for a database", arity = 1)
    public int maxNumTableAlters;

    @Parameter(names = "--test-engine-type", description = "The engine type in Doris, only consider OLAP now", arity = 1)
    public String testEngineType = "OLAP"; // skip now

    @Parameter(names = "--test-indexes", description = "Allow explicit indexes, Doris only supports creating indexes on single-column BITMAP", arity = 1)
    public boolean testIndexes = true; // skip now

    @Parameter(names = "--test-column-aggr", description = "Allow test column aggregation (sum, min, max, replace, replace_if_not_null, hll_union, bitmap_untion)", arity = 1)
    public boolean testColumnAggr = true;

    @Parameter(names = "--test-datemodel", description = "Allow generating Doris’s data model in tables. (Aggregate、Uniqe、Duplicate)", arity = 1)
    public boolean testDataModel = true;

    @Parameter(names = "--test-distribution", description = "Allow generating data distribution in tables.", arity = 1)
    public boolean testDistribution = true; // must have it, skip now

    @Parameter(names = "--test-rollup", description = "Allow generating rollups in tables.", arity = 1)
    public boolean testRollup = true; // skip now

    @Parameter(names = "--oracle")
    public List<DorisOracleFactory> oracles = Arrays.asList(DorisOracleFactory.NOREC);

    public enum DorisOracleFactory implements OracleFactory<DorisGlobalState> {
        NOREC {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws SQLException {
                return new DorisNoRECOracle(globalState);
            }

        },
        HAVING {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws SQLException {
                return new DorisQueryPartitioningHavingTester(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws SQLException {
                return new DorisQueryPartitioningWhereTester(globalState);
            }
        },
        GROUP_BY {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws SQLException {
                return new DorisQueryPartitioningGroupByTester(globalState);
            }
        },
        AGGREGATE {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws SQLException {
                return new DorisQueryPartitioningAggregateTester(globalState);
            }

        },
        DISTINCT {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws SQLException {
                return new DorisQueryPartitioningDistinctTester(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws SQLException {
                List<TestOracle<DorisGlobalState>> oracles = new ArrayList<>();
                oracles.add(new DorisQueryPartitioningWhereTester(globalState));
                oracles.add(new DorisQueryPartitioningHavingTester(globalState));
                oracles.add(new DorisQueryPartitioningAggregateTester(globalState));
                oracles.add(new DorisQueryPartitioningDistinctTester(globalState));
                oracles.add(new DorisQueryPartitioningGroupByTester(globalState));
                return new CompositeTestOracle<DorisGlobalState>(oracles, globalState);
            }
        },
        PQS {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws Exception {
                return new DorisPivotedQuerySynthesisOracle(globalState);
            }
        },
        ALL {
            @Override
            public TestOracle<DorisGlobalState> create(DorisGlobalState globalState) throws Exception {
                List<TestOracle<DorisGlobalState>> oracles = new ArrayList<>();
                oracles.add(new DorisNoRECOracle(globalState));
                oracles.add(new DorisQueryPartitioningWhereTester(globalState));
                oracles.add(new DorisQueryPartitioningHavingTester(globalState));
                oracles.add(new DorisQueryPartitioningAggregateTester(globalState));
                oracles.add(new DorisQueryPartitioningDistinctTester(globalState));
                oracles.add(new DorisQueryPartitioningGroupByTester(globalState));
                oracles.add(new DorisPivotedQuerySynthesisOracle(globalState));
                return new CompositeTestOracle<DorisGlobalState>(oracles, globalState);
            }
        }

    }

    @Override
    public List<DorisOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
