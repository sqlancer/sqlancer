package sqlancer.stonedb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.stonedb.StoneDBOptions.StoneDBOracleFactory;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.oracle.StoneDBAggregateOracle;
import sqlancer.stonedb.oracle.StoneDBNoRECOracle;
import sqlancer.stonedb.oracle.StoneDBQueryPartitioningDistinctTester;
import sqlancer.stonedb.oracle.StoneDBQueryPartitioningGroupByTester;
import sqlancer.stonedb.oracle.StoneDBQueryPartitioningHavingTester;
import sqlancer.stonedb.oracle.StoneDBQueryPartitioningWhereTester;

@Parameters(separators = "=", commandDescription = "StoneDB (default host: " + StoneDBOptions.DEFAULT_HOST
        + ", default port: " + StoneDBOptions.DEFAULT_PORT + ")")
public class StoneDBOptions implements DBMSSpecificOptions<StoneDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<StoneDBOracleFactory> oracles = List.of(StoneDBOracleFactory.NOREC);

    public enum StoneDBOracleFactory implements OracleFactory<StoneDBGlobalState> {
        NOREC {
            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws SQLException {
                return new StoneDBNoRECOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws SQLException {
                List<TestOracle<StoneDBGlobalState>> oracles = new ArrayList<>();
                oracles.add(new StoneDBQueryPartitioningWhereTester(globalState));
                oracles.add(new StoneDBQueryPartitioningHavingTester(globalState));
                oracles.add(new StoneDBAggregateOracle(globalState));
                oracles.add(new StoneDBQueryPartitioningDistinctTester(globalState));
                oracles.add(new StoneDBQueryPartitioningGroupByTester(globalState));
                return new CompositeTestOracle<>(oracles, globalState);
            }
        },
        HAVING {
            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws SQLException {
                return new StoneDBQueryPartitioningHavingTester(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws SQLException {
                return new StoneDBQueryPartitioningWhereTester(globalState);
            }
        },
        GROUP_BY {
            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws SQLException {
                return new StoneDBQueryPartitioningGroupByTester(globalState);
            }
        },
        AGGREGATE {

            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws SQLException {
                return new StoneDBAggregateOracle(globalState);
            }

        },
        DISTINCT {
            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws SQLException {
                return new StoneDBQueryPartitioningDistinctTester(globalState);
            }
        }
    }

    @Override
    public List<StoneDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }
}
