package sqlancer.doris;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.doris.gen.DorisNewExpressionGenerator;
import sqlancer.doris.oracle.DorisPivotedQuerySynthesisOracle;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningAggregateTester;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningDistinctTester;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningGroupByTester;
import sqlancer.doris.oracle.tlp.DorisQueryPartitioningHavingTester;

public enum DorisOracleFactory implements OracleFactory<DorisProvider.DorisGlobalState> {
    NOREC {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws SQLException {
            DorisNewExpressionGenerator gen = new DorisNewExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(DorisErrors.getExpressionErrors())
                    .with("canceling statement due to statement timeout").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }

    },
    HAVING {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws SQLException {
            return new DorisQueryPartitioningHavingTester(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws SQLException {
            DorisNewExpressionGenerator gen = new DorisNewExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(DorisErrors.getExpressionErrors())
                    .with(DorisErrors.getExpressionErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws SQLException {
            return new DorisQueryPartitioningGroupByTester(globalState);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws SQLException {
            return new DorisQueryPartitioningAggregateTester(globalState);
        }

    },
    DISTINCT {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws SQLException {
            return new DorisQueryPartitioningDistinctTester(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws Exception {
            List<TestOracle<DorisProvider.DorisGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            return new CompositeTestOracle<DorisProvider.DorisGlobalState>(oracles, globalState);
        }
    },
    PQS {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws Exception {
            return new DorisPivotedQuerySynthesisOracle(globalState);
        }
    },
    ALL {
        @Override
        public TestOracle<DorisProvider.DorisGlobalState> create(DorisProvider.DorisGlobalState globalState)
                throws Exception {
            List<TestOracle<DorisProvider.DorisGlobalState>> oracles = new ArrayList<>();
            oracles.add(NOREC.create(globalState));
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            oracles.add(new DorisPivotedQuerySynthesisOracle(globalState));
            return new CompositeTestOracle<DorisProvider.DorisGlobalState>(oracles, globalState);
        }
    }

}
