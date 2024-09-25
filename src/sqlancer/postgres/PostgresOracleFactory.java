package sqlancer.postgres;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.PostgresCERTOracle;
import sqlancer.postgres.oracle.PostgresFuzzer;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle;

public enum PostgresOracleFactory implements OracleFactory<PostgresGlobalState> {
    NOREC {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                    .with(PostgresCommon.getCommonFetchErrors())
                    .withRegex(PostgresCommon.getCommonExpressionRegexErrors()).build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    PQS {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            return new PostgresPivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    WHERE {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                    .with(PostgresCommon.getCommonFetchErrors())
                    .withRegex(PostgresCommon.getCommonExpressionRegexErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }

    },
    HAVING {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            return new PostgresTLPHavingOracle(globalState);
        }

    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws Exception {
            List<TestOracle<PostgresGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(new PostgresTLPAggregateOracle(globalState));
            return new CompositeTestOracle<PostgresGlobalState>(oracles, globalState);
        }
    },
    CERT {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            return new PostgresCERTOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    FUZZER {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws Exception {
            return new PostgresFuzzer(globalState);
        }

    };

}
