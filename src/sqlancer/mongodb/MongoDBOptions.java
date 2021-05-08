package sqlancer.mongodb;

import static sqlancer.mongodb.MongoDBOptions.MongoDBOracleFactory.DOCUMENT_REMOVAL;
import static sqlancer.mongodb.MongoDBOptions.MongoDBOracleFactory.QUERY_PARTITIONING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mongodb.test.MongoDBDocumentRemovalTester;
import sqlancer.mongodb.test.MongoDBQueryPartitioningWhereTester;

public class MongoDBOptions implements DBMSSpecificOptions<MongoDBOptions.MongoDBOracleFactory> {

    @Parameter(names = "--test-validation", description = "Enable/Disable validation of schema with Schema Validation", arity = 1)
    public boolean testValidation = true;

    @Parameter(names = "--test-null-inserts", description = "Enables to test inserting with null values, validation has to be off", arity = 1)
    public boolean testNullInserts;

    @Parameter(names = "--test-random-types", description = "Insert random types instead of schema types, validation has to be off", arity = 1)
    public boolean testRandomTypes;

    @Parameter(names = "--max-number-indexes", description = "The maximum number of indexes used.", arity = 1)
    public int maxNumberIndexes = 15;

    @Parameter(names = "--test-computed-values", description = "Enable adding computed values to query", arity = 1)
    public boolean testComputedValues;

    @Parameter(names = "--test-with-regex", description = "Enable Regex Leaf Nodes", arity = 1)
    public boolean testWithRegex;

    @Parameter(names = "--test-with-count", description = "Count the number of documents and check with count command", arity = 1)
    public boolean testWithCount;

    @Parameter(names = "--null-safety", description = "", arity = 1)
    public boolean nullSafety;

    @Parameter(names = "--oracle")
    public List<MongoDBOracleFactory> oracles = Arrays.asList(QUERY_PARTITIONING, DOCUMENT_REMOVAL);

    @Override
    public List<MongoDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

    public enum MongoDBOracleFactory implements OracleFactory<MongoDBProvider.MongoDBGlobalState> {
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(MongoDBProvider.MongoDBGlobalState globalState) throws Exception {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new MongoDBQueryPartitioningWhereTester(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        },
        DOCUMENT_REMOVAL {
            @Override
            public TestOracle create(MongoDBProvider.MongoDBGlobalState globalState) throws Exception {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new MongoDBDocumentRemovalTester(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        }
    }
}
