package sqlancer.presto;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.presto.test.PrestoNoRECOracle;
import sqlancer.presto.test.PrestoQueryPartitioningAggregateTester;
import sqlancer.presto.test.PrestoQueryPartitioningDistinctTester;
import sqlancer.presto.test.PrestoQueryPartitioningGroupByTester;
import sqlancer.presto.test.PrestoQueryPartitioningHavingTester;
import sqlancer.presto.test.PrestoQueryPartitioningWhereTester;

@Parameters(commandDescription = "Presto")
public class PrestoOptions implements DBMSSpecificOptions<PrestoOptions.PrestoOracleFactory> {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8080;

    @Parameter(names = "--test-collate", arity = 1)
    public boolean testCollate = true;

    @Parameter(names = "--test-check", description = "Allow generating CHECK constraints in tables", arity = 1)
    public boolean testCheckConstraints = true;

    @Parameter(names = "--test-default-values", description = "Allow generating DEFAULT values in tables", arity = 1)
    public boolean testDefaultValues = true;

    @Parameter(names = "--test-not-null", description = "Allow generating NOT NULL constraints in tables", arity = 1)
    public boolean testNotNullConstraints = true;

    @Parameter(names = "--test-functions", description = "Allow generating functions in expressions", arity = 1)
    public boolean testFunctions = true;

    @Parameter(names = "--test-casts", description = "Allow generating casts in expressions", arity = 1)
    public boolean testCasts = true;

    @Parameter(names = "--test-between", description = "Allow generating the BETWEEN operator in expressions (FALSE by default : Presto null handling in BETWEEN operator : https://prestodb.io/docs/current/functions/comparison.html )", arity = 1)
    public boolean testBetween;

    @Parameter(names = "--test-in", description = "Allow generating the IN operator in expressions", arity = 1)
    public boolean testIn = true;

    @Parameter(names = "--test-case", description = "Allow generating the CASE operator in expressions", arity = 1)
    public boolean testCase = true;

    @Parameter(names = "--test-binary-logicals", description = "Allow generating AND and OR in expressions", arity = 1)
    public boolean testBinaryLogicals = true;

    @Parameter(names = "--test-int-constants", description = "Allow generating INTEGER constants", arity = 1)
    public boolean testIntConstants = true;

    @Parameter(names = "--test-varchar-constants", description = "Allow generating VARCHAR constants", arity = 1)
    public boolean testStringConstants = true;

    @Parameter(names = "--test-time-constants", description = "Allow generating DATE constants", arity = 1)
    public boolean testDateConstants = true;

    @Parameter(names = "--test-date-constants", description = "Allow generating DATE constants", arity = 1)
    public boolean testTimeConstants = true;

    @Parameter(names = "--test-timestamp-constants", description = "Allow generating TIMESTAMP constants", arity = 1)
    public boolean testTimestampConstants = true;

    @Parameter(names = "--test-float-constants", description = "Allow generating floating-point constants", arity = 1)
    public boolean testFloatConstants = true;

    @Parameter(names = "--test-boolean-constants", description = "Allow generating boolean constants", arity = 1)
    public boolean testBooleanConstants = true;

    @Parameter(names = "--test-binary-comparisons", description = "Allow generating binary comparison operators (e.g., >= or LIKE)", arity = 1)
    public boolean testBinaryComparisons = true;

    @Parameter(names = "--test-indexes", description = "Allow explicit (i.e. CREATE INDEX) and implicit (i.e., UNIQUE and PRIMARY KEY) indexes", arity = 1)
    public boolean testIndexes = true;

    @Parameter(names = "--test-rowid", description = "Test tables' rowid columns", arity = 1)
    public boolean testRowid = true;

    @Parameter(names = "--max-num-views", description = "The maximum number of views that can be generated for a database", arity = 1)
    public int maxNumViews = 1;

    @Parameter(names = "--max-num-deletes", description = "The maximum number of DELETE statements that are issued for a database", arity = 1)
    public int maxNumDeletes = 1;

    @Parameter(names = "--max-num-updates", description = "The maximum number of UPDATE statements that are issued for a database", arity = 1)
    public int maxNumUpdates = 5;

    @Parameter(names = "--oracle")
    public List<PrestoOracleFactory> oracles = List.of(PrestoOracleFactory.NOREC);

    @Parameter(names = "--catalog")
    public String catalog = "memory";

    @Parameter(names = "--schema")
    public String schema = "test";

    @Parameter(names = "--typed-generator", description = "the expression generator type - typed and untyped ")
    public boolean typedGenerator = true;

    @Override
    public List<PrestoOracleFactory> getTestOracleFactory() {
        return oracles;
    }

    public enum PrestoOracleFactory implements OracleFactory<PrestoGlobalState> {
        NOREC {
            @Override
            public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
                return new PrestoNoRECOracle(globalState);
            }

        },
        HAVING {
            @Override
            public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
                return new PrestoQueryPartitioningHavingTester(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
                return new PrestoQueryPartitioningWhereTester(globalState);
            }
        },
        GROUP_BY {
            @Override
            public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
                return new PrestoQueryPartitioningGroupByTester(globalState);
            }
        },
        AGGREGATE {
            @Override
            public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
                return new PrestoQueryPartitioningAggregateTester(globalState);
            }

        },
        DISTINCT {
            @Override
            public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
                return new PrestoQueryPartitioningDistinctTester(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
                List<TestOracle<PrestoGlobalState>> oracles = new ArrayList<>();
                oracles.add(new PrestoQueryPartitioningWhereTester(globalState));
                oracles.add(new PrestoQueryPartitioningHavingTester(globalState));
                oracles.add(new PrestoQueryPartitioningAggregateTester(globalState));
                oracles.add(new PrestoQueryPartitioningDistinctTester(globalState));
                oracles.add(new PrestoQueryPartitioningGroupByTester(globalState));
                return new CompositeTestOracle<>(oracles, globalState);
            }
        }

    }

}
