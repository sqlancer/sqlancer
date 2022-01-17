package sqlancer.postgres;

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
import sqlancer.postgres.PostgresOptions.PostgresOracleFactory;
import sqlancer.postgres.oracle.PostgresNoRECOracle;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPWhereOracle;

@Parameters(separators = "=", commandDescription = "PostgreSQL (default port: " + PostgresOptions.DEFAULT_PORT
        + ", default host: " + PostgresOptions.DEFAULT_HOST + ")")
public class PostgresOptions implements DBMSSpecificOptions<PostgresOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5432;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<PostgresOracleFactory> oracle = Arrays.asList(PostgresOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the PostgreSQL server", arity = 1)
    public String connectionURL = String.format("postgresql://%s:%d/test", PostgresOptions.DEFAULT_HOST,
            PostgresOptions.DEFAULT_PORT);

    public enum PostgresOracleFactory implements OracleFactory<PostgresGlobalState> {
        NOREC {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                return new PostgresNoRECOracle(globalState);
            }
        },
        PQS {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                return new PostgresPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        HAVING {

            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                return new PostgresTLPHavingOracle(globalState);
            }

        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new PostgresTLPWhereOracle(globalState));
                oracles.add(new PostgresTLPHavingOracle(globalState));
                oracles.add(new PostgresTLPAggregateOracle(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };

    }

    @Override
    public List<PostgresOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
