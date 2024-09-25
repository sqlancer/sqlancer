package sqlancer.databend;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.databend.gen.DatabendNewExpressionGenerator;
import sqlancer.databend.test.DatabendPivotedQuerySynthesisOracle;
import sqlancer.databend.test.tlp.DatabendQueryPartitioningAggregateTester;
import sqlancer.databend.test.tlp.DatabendQueryPartitioningDistinctTester;
import sqlancer.databend.test.tlp.DatabendQueryPartitioningGroupByTester;
import sqlancer.databend.test.tlp.DatabendQueryPartitioningHavingTester;

public enum DatabendOracleFactory implements OracleFactory<DatabendProvider.DatabendGlobalState> {
    NOREC {
        @Override
        public TestOracle<DatabendProvider.DatabendGlobalState> create(DatabendProvider.DatabendGlobalState globalState)
                throws SQLException {
            DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(DatabendErrors.getExpressionErrors())
                    .with("canceling statement due to statement timeout").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }

    },
    HAVING {
        @Override
        public TestOracle<DatabendProvider.DatabendGlobalState> create(DatabendProvider.DatabendGlobalState globalState)
                throws SQLException {
            return new DatabendQueryPartitioningHavingTester(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<DatabendProvider.DatabendGlobalState> create(DatabendProvider.DatabendGlobalState globalState)
                throws SQLException {
            DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(DatabendErrors.getExpressionErrors())
                    .with(DatabendErrors.getGroupByErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<DatabendProvider.DatabendGlobalState> create(DatabendProvider.DatabendGlobalState globalState)
                throws SQLException {
            return new DatabendQueryPartitioningGroupByTester(globalState);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<DatabendProvider.DatabendGlobalState> create(DatabendProvider.DatabendGlobalState globalState)
                throws SQLException {
            return new DatabendQueryPartitioningAggregateTester(globalState);
        }

    },
    DISTINCT {
        @Override
        public TestOracle<DatabendProvider.DatabendGlobalState> create(DatabendProvider.DatabendGlobalState globalState)
                throws SQLException {
            return new DatabendQueryPartitioningDistinctTester(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<DatabendProvider.DatabendGlobalState> create(DatabendProvider.DatabendGlobalState globalState)
                throws Exception {
            List<TestOracle<DatabendProvider.DatabendGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            return new CompositeTestOracle<DatabendProvider.DatabendGlobalState>(oracles, globalState);
        }
    },
    PQS {
        @Override
        public TestOracle<DatabendProvider.DatabendGlobalState> create(DatabendProvider.DatabendGlobalState globalState)
                throws Exception {
            return new DatabendPivotedQuerySynthesisOracle(globalState);
        }
    }

}
