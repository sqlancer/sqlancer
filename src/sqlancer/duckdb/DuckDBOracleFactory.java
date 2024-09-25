package sqlancer.duckdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;
import sqlancer.duckdb.test.DuckDBQueryPartitioningAggregateTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningDistinctTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningGroupByTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningHavingTester;

public enum DuckDBOracleFactory implements OracleFactory<DuckDBProvider.DuckDBGlobalState> {
    NOREC {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(DuckDBErrors.getExpressionErrors())
                    .withRegex(DuckDBErrors.getExpressionErrorsRegex())
                    .with("canceling statement due to statement timeout").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }

    },
    HAVING {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            return new DuckDBQueryPartitioningHavingTester(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(DuckDBErrors.getExpressionErrors())
                    .with(DuckDBErrors.getGroupByErrors()).withRegex(DuckDBErrors.getExpressionErrorsRegex()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            return new DuckDBQueryPartitioningGroupByTester(globalState);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            return new DuckDBQueryPartitioningAggregateTester(globalState);
        }

    },
    DISTINCT {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            return new DuckDBQueryPartitioningDistinctTester(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws Exception {
            List<TestOracle<DuckDBProvider.DuckDBGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            return new CompositeTestOracle<DuckDBProvider.DuckDBGlobalState>(oracles, globalState);
        }
    };

}
