package sqlancer.arangodb;

import static sqlancer.arangodb.ArangoDBOptions.ArangoDBOracleFactory.QUERY_PARTITIONING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.arangodb.ArangoDBProvider.ArangoDBGlobalState;
import sqlancer.arangodb.test.ArangoDBQueryPartitioningWhereTester;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;

@Parameters(commandDescription = "ArangoDB (experimental)")
public class ArangoDBOptions implements DBMSSpecificOptions<ArangoDBOptions.ArangoDBOracleFactory> {

    @Parameter(names = "--oracle")
    public List<ArangoDBOracleFactory> oracles = Arrays.asList(QUERY_PARTITIONING);

    @Parameter(names = "--test-random-type-inserts", description = "Insert random types instead of schema types.")
    public boolean testRandomTypeInserts;

    @Parameter(names = "--max-number-indexes", description = "The maximum number of indexes used.", arity = 1)
    public int maxNumberIndexes = 15;

    @Parameter(names = "--with-optimizer-rule-tests", description = "Adds an additional query, where a random set"
            + "of optimizer rules are disabled.", arity = 1)
    public boolean withOptimizerRuleTests;

    @Override
    public List<ArangoDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

    public enum ArangoDBOracleFactory implements OracleFactory<ArangoDBGlobalState> {
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(ArangoDBGlobalState globalState) throws Exception {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new ArangoDBQueryPartitioningWhereTester(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        }
    }
}
