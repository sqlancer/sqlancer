package sqlancer.tidb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CERTOracle;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLancerResultSet;
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
            TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(TiDBErrors.getExpressionErrors()).build();
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<Long>> rowCountParser = (rs) -> {
                String content = rs.getString(2);
                return Optional.of((long) Double.parseDouble(content));
            };
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<String>> queryPlanParser = (rs) -> {
                String operation = rs.getString(1).split("_")[0]; // Extract operation names for query plans
                return Optional.of(operation);
            };

            return new CERTOracle<>(globalState, gen, expectedErrors, rowCountParser, queryPlanParser);
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
