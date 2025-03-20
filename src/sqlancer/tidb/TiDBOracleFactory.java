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
                try {
                    String content = rs.getString(2);
                    if (content == null || content.trim().isEmpty()) {
                        return Optional.empty();
                    }

                    String numStr = content.replaceAll("[^0-9.-]", "");
                    if (!numStr.isEmpty()) {
                        return Optional.of((long) Double.parseDouble(numStr));
                    }
                } catch (Exception e) {
                    // Ignore parsing erors
                }
                return Optional.empty();
            };
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<String>> queryPlanParser = (rs) -> {
                try {
                    String operation = rs.getString(1);
                    if (operation == null || operation.trim().isEmpty()) {
                        return Optional.empty();
                    }
                    // Extract operation name and handle TiDB's specific format
                    String[] parts = operation.split("_");
                    if (parts.length > 0) {
                        return Optional.of(parts[0].toLowerCase());
                    }
                } catch (Exception e) {
                    // Ignore parsing errors
                }
                return Optional.empty();
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