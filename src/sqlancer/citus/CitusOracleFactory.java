package sqlancer.citus;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.citus.oracle.tlp.CitusTLPAggregateOracle;
import sqlancer.citus.oracle.tlp.CitusTLPHavingOracle;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;

public enum CitusOracleFactory implements OracleFactory<PostgresGlobalState> {
    NOREC {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                    .with(PostgresCommon.getCommonFetchErrors())
                    .withRegex(PostgresCommon.getCommonExpressionRegexErrors())
                    .with(CitusCommon.getCitusErrors().toArray(new String[0])).build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    PQS {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            return new PostgresPivotedQuerySynthesisOracle(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                    .with(PostgresCommon.getCommonFetchErrors())
                    .withRegex(PostgresCommon.getCommonExpressionRegexErrors()).with(CitusCommon.getCitusErrors())
                    .build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    HAVING {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            CitusGlobalState citusGlobalState = (CitusGlobalState) globalState;
            return new CitusTLPHavingOracle(citusGlobalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws Exception {
            CitusGlobalState citusGlobalState = (CitusGlobalState) globalState;
            List<TestOracle<PostgresGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(citusGlobalState));
            oracles.add(HAVING.create(citusGlobalState));
            oracles.add(new CitusTLPAggregateOracle(citusGlobalState));
            return new CompositeTestOracle<PostgresGlobalState>(oracles, globalState);
        }
    };

}
