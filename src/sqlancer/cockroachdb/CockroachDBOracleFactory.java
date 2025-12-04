package sqlancer.cockroachdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPAggregateOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPDistinctOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPExtendedWhereOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPGroupByOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPHavingOracle;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

public enum CockroachDBOracleFactory implements OracleFactory<CockroachDBProvider.CockroachDBGlobalState> {
    NOREC {
        @Override
        public TestOracle<CockroachDBProvider.CockroachDBGlobalState> create(
                CockroachDBProvider.CockroachDBGlobalState globalState) throws SQLException {
            CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(CockroachDBErrors.getExpressionErrors())
                    .with(CockroachDBErrors.getTransactionErrors()).with("unable to vectorize execution plan") // SET
                                                                                                               // vectorize=experimental_always;
                    .with(" mismatched physical types at index") // SET vectorize=experimental_always;
                    .build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<CockroachDBProvider.CockroachDBGlobalState> create(
                CockroachDBProvider.CockroachDBGlobalState globalState) throws SQLException {
            return new CockroachDBTLPAggregateOracle(globalState);
        }

    },
    GROUP_BY {
        @Override
        public TestOracle<CockroachDBProvider.CockroachDBGlobalState> create(
                CockroachDBProvider.CockroachDBGlobalState globalState) throws SQLException {
            return new CockroachDBTLPGroupByOracle(globalState);
        }
    },
    HAVING {
        @Override
        public TestOracle<CockroachDBProvider.CockroachDBGlobalState> create(
                CockroachDBProvider.CockroachDBGlobalState globalState) throws SQLException {
            return new CockroachDBTLPHavingOracle(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<CockroachDBProvider.CockroachDBGlobalState> create(
                CockroachDBProvider.CockroachDBGlobalState globalState) throws SQLException {
            CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(CockroachDBErrors.getExpressionErrors())
                    .with("GROUP BY term out of range").build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    DISTINCT {
        @Override
        public TestOracle<CockroachDBProvider.CockroachDBGlobalState> create(
                CockroachDBProvider.CockroachDBGlobalState globalState) throws SQLException {
            return new CockroachDBTLPDistinctOracle(globalState);
        }
    },
    EXTENDED_WHERE {
        @Override
        public TestOracle<CockroachDBProvider.CockroachDBGlobalState> create(
                CockroachDBProvider.CockroachDBGlobalState globalState) throws SQLException {
            return new CockroachDBTLPExtendedWhereOracle(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<CockroachDBProvider.CockroachDBGlobalState> create(
                CockroachDBProvider.CockroachDBGlobalState globalState) throws Exception {
            List<TestOracle<CockroachDBProvider.CockroachDBGlobalState>> oracles = new ArrayList<>();
            oracles.add(AGGREGATE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(WHERE.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            oracles.add(EXTENDED_WHERE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            return new CompositeTestOracle<CockroachDBProvider.CockroachDBGlobalState>(oracles, globalState);
        }
    };

}
