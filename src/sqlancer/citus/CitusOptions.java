package sqlancer.citus;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.OracleFactory;
import sqlancer.citus.oracle.CitusNoRECOracle;
import sqlancer.citus.oracle.tlp.CitusTLPAggregateOracle;
import sqlancer.citus.oracle.tlp.CitusTLPHavingOracle;
import sqlancer.citus.oracle.tlp.CitusTLPWhereOracle;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;

public class CitusOptions extends PostgresOptions {

    @Parameter(names = "--repartition", description = "Specifies whether repartition joins should be allowed", arity = 1)
    public boolean repartition = true;

    @Parameter(names = "--citusoracle", description = "Specifies which test oracle should be used for Citus extension to PostgreSQL")
    public List<CitusOracleFactory> citusOracle = Arrays.asList(CitusOracleFactory.QUERY_PARTITIONING);

    public enum CitusOracleFactory implements OracleFactory<PostgresGlobalState> {
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

    }

}
