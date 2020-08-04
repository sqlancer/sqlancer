package sqlancer.citus;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;
import sqlancer.CompositeTestOracle;
import sqlancer.TestOracle;
import sqlancer.citus.oracle.CitusNoRECOracle;
import sqlancer.citus.oracle.tlp.CitusTLPAggregateOracle;
import sqlancer.citus.oracle.tlp.CitusTLPHavingOracle;
import sqlancer.citus.oracle.tlp.CitusTLPWhereOracle;

public class CitusOptions extends PostgresOptions {

    @Parameter(names = "--repartition")
    public boolean repartition = true;
    
    /* @Override
    @Parameter(names = "--citusOracle")
    public List<CitusOracle> oracle = Arrays.asList(CitusOracle.QUERY_PARTITIONING);

    @Override */
    public enum PostgresOracle {
        NOREC {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                CitusGlobalState citusGlobalState = (CitusGlobalState) globalState;
                return new CitusNoRECOracle(citusGlobalState);
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
                CitusGlobalState citusGlobalState = (CitusGlobalState) globalState;
                return new CitusTLPHavingOracle(citusGlobalState);
            }

        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                CitusGlobalState citusGlobalState = (CitusGlobalState) globalState;
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new CitusTLPWhereOracle(citusGlobalState));
                oracles.add(new CitusTLPHavingOracle(citusGlobalState));
                oracles.add(new CitusTLPAggregateOracle(citusGlobalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };

        public abstract TestOracle create(PostgresGlobalState globalState) throws SQLException;

    }

}
