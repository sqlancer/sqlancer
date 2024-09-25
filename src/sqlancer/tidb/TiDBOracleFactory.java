package sqlancer.tidb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.tidb.oracle.TiDBCERTOracle;
import sqlancer.tidb.oracle.TiDBDQPOracle;
import sqlancer.tidb.oracle.TiDBTLPHavingOracle;

public enum TiDBOracleFactory implements OracleFactory<TiDBProvider.TiDBGlobalState> {
    HAVING {
        @Override
        public TestOracle<TiDBProvider.TiDBGlobalState> create(TiDBProvider.TiDBGlobalState globalState)
                throws SQLException {
            return new TiDBTLPHavingOracle(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<TiDBProvider.TiDBGlobalState> create(TiDBProvider.TiDBGlobalState globalState)
                throws SQLException {
            TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(TiDBErrors.getExpressionErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<TiDBProvider.TiDBGlobalState> create(TiDBProvider.TiDBGlobalState globalState)
                throws Exception {
            List<TestOracle<TiDBProvider.TiDBGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            return new CompositeTestOracle<TiDBProvider.TiDBGlobalState>(oracles, globalState);
        }
    },
    CERT {
        @Override
        public TestOracle<TiDBProvider.TiDBGlobalState> create(TiDBProvider.TiDBGlobalState globalState)
                throws SQLException {
            return new TiDBCERTOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    DQP {
        @Override
        public TestOracle<TiDBProvider.TiDBGlobalState> create(TiDBProvider.TiDBGlobalState globalState)
                throws SQLException {
            return new TiDBDQPOracle(globalState);
        }
    };

}
