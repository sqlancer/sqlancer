package sqlancer.presto;

import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;
import sqlancer.presto.test.PrestoQueryPartitioningAggregateTester;
import sqlancer.presto.test.PrestoQueryPartitioningDistinctTester;
import sqlancer.presto.test.PrestoQueryPartitioningGroupByTester;
import sqlancer.presto.test.PrestoQueryPartitioningHavingTester;
import sqlancer.presto.test.PrestoQueryPartitioningWhereTester;

public enum PrestoOracleFactory implements OracleFactory<PrestoGlobalState> {
    NOREC {
        @Override
        public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
            PrestoTypedExpressionGenerator gen = new PrestoTypedExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(PrestoErrors.getExpressionErrors())
                    .with("canceling statement due to statement timeout").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }

    },
    HAVING {
        @Override
        public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
            return new PrestoQueryPartitioningHavingTester(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
            return new PrestoQueryPartitioningWhereTester(globalState);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
            return new PrestoQueryPartitioningGroupByTester(globalState);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
            return new PrestoQueryPartitioningAggregateTester(globalState);
        }

    },
    DISTINCT {
        @Override
        public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) {
            return new PrestoQueryPartitioningDistinctTester(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<PrestoGlobalState> create(PrestoGlobalState globalState) throws Exception {
            List<TestOracle<PrestoGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            return new CompositeTestOracle<>(oracles, globalState);
        }
    }

}
