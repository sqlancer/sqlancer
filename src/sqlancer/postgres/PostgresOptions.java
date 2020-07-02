package sqlancer.postgres;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.CompositeTestOracle;
import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.postgres.oracle.PostgresNoRECOracle;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPWhereOracle;

@Parameters
public class PostgresOptions {

    @Parameter(names = "--bulk-insert")
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", converter = DBMSConverter.class)
    public List<PostgresOracle> oracle = Arrays.asList(PostgresOracle.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", arity = 1)
    public boolean testCollations = true;

    public enum PostgresOracle {
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
                return new CompositeTestOracle(oracles);
            }
        };

        public abstract TestOracle create(PostgresGlobalState globalState) throws SQLException;

    }

}
