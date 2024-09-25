package sqlancer.yugabyte.ysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;
import sqlancer.yugabyte.ysql.oracle.YSQLCatalog;
import sqlancer.yugabyte.ysql.oracle.YSQLFuzzer;
import sqlancer.yugabyte.ysql.oracle.YSQLPivotedQuerySynthesisOracle;
import sqlancer.yugabyte.ysql.oracle.tlp.YSQLTLPAggregateOracle;
import sqlancer.yugabyte.ysql.oracle.tlp.YSQLTLPHavingOracle;
import sqlancer.yugabyte.ysql.oracle.tlp.YSQLTLPWhereOracle;

public enum YSQLOracleFactory implements OracleFactory<YSQLGlobalState> {
    FUZZER {
        @Override
        public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
            return new YSQLFuzzer(globalState);
        }
    },
    CATALOG {
        @Override
        public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
            return new YSQLCatalog(globalState);
        }
    },
    NOREC {
        @Override
        public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
            YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(YSQLErrors.getCommonExpressionErrors())
                    .with(YSQLErrors.getCommonFetchErrors()).with("canceling statement due to statement timeout")
                    .build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    PQS {
        @Override
        public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
            return new YSQLPivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    HAVING {
        @Override
        public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
            return new YSQLTLPHavingOracle(globalState);
        }

    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<YSQLGlobalState> create(YSQLGlobalState globalState) throws SQLException {
            List<TestOracle<YSQLGlobalState>> oracles = new ArrayList<>();
            oracles.add(new YSQLTLPWhereOracle(globalState));
            oracles.add(new YSQLTLPHavingOracle(globalState));
            oracles.add(new YSQLTLPAggregateOracle(globalState));
            return new CompositeTestOracle<YSQLGlobalState>(oracles, globalState);
        }
    }

}
