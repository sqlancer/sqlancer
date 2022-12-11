package sqlancer.yugabyte.ysql;

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
import sqlancer.yugabyte.ysql.YSQLOptions.YSQLOracleFactory;
import sqlancer.yugabyte.ysql.oracle.YSQLCatalog;
import sqlancer.yugabyte.ysql.oracle.YSQLFuzzer;
import sqlancer.yugabyte.ysql.oracle.YSQLNoRECOracle;
import sqlancer.yugabyte.ysql.oracle.YSQLPivotedQuerySynthesisOracle;
import sqlancer.yugabyte.ysql.oracle.tlp.YSQLTLPAggregateOracle;
import sqlancer.yugabyte.ysql.oracle.tlp.YSQLTLPHavingOracle;
import sqlancer.yugabyte.ysql.oracle.tlp.YSQLTLPWhereOracle;

@Parameters(separators = "=", commandDescription = "YSQL (default port: " + YSQLOptions.DEFAULT_PORT
        + ", default host: " + YSQLOptions.DEFAULT_HOST)
public class YSQLOptions implements DBMSSpecificOptions<YSQLOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5433;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for YSQL")
    public List<YSQLOracleFactory> oracle = Arrays.asList(YSQLOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the YSQL server", arity = 1)
    public String connectionURL = String.format("jdbc:yugabytedb://%s:%d/yugabyte", YSQLOptions.DEFAULT_HOST,
            YSQLOptions.DEFAULT_PORT);

    @Override
    public List<YSQLOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public enum YSQLOracleFactory implements OracleFactory<YSQLGlobalState> {
        FUZZER {
            @Override
            public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
                return new YSQLFuzzer(globalState);
            }
        },
        CATALOG {
            @Override
            public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
                return new YSQLCatalog(globalState);
            }
        },
        NOREC {
            @Override
            public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
                return new YSQLNoRECOracle(globalState);
            }
        },
        PQS {
            @Override
            public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
                return new YSQLPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        HAVING {
            @Override
            public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
                return new YSQLTLPHavingOracle(globalState);
            }

        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
                List<TestOracle<YSQLGlobalState>> oracles = new ArrayList<>();
                oracles.add(new YSQLTLPWhereOracle(globalState));
                oracles.add(new YSQLTLPHavingOracle(globalState));
                oracles.add(new YSQLTLPAggregateOracle(globalState));
                return new CompositeTestOracle<YSQLGlobalState>(oracles, globalState);
            }
        }

    }

}
