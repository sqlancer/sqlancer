package sqlancer.oxla;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.oxla.gen.OxlaExpressionGenerator;
import sqlancer.oxla.oracle.*;

import java.util.ArrayList;
import java.util.List;

public enum OxlaOracleFactory implements OracleFactory<OxlaGlobalState> {
    NOREC {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            OxlaExpressionGenerator generator = new OxlaExpressionGenerator(globalState);
            return new NoRECOracle<>(globalState, generator, OxlaCommon.ALL_ERRORS);
        }
    },
    PQS {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaPivotedQuerySynthesisOracle(globalState, OxlaCommon.ALL_ERRORS);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaTLPAggregateOracle(globalState, OxlaCommon.ALL_ERRORS);
        }
    },
    DISTINCT {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaTLPDistinctOracle(globalState, OxlaCommon.ALL_ERRORS);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaTLPGroupByOracle(globalState, OxlaCommon.ALL_ERRORS);
        }
    },
    HAVING {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaTLPHavingOracle(globalState, OxlaCommon.ALL_ERRORS);
        }
    },
    WHERE {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            OxlaExpressionGenerator generator = new OxlaExpressionGenerator(globalState);
            return new TLPWhereOracle<>(globalState, generator, OxlaCommon.ALL_ERRORS);
        }
    },
    WHERE_EXTENDED {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaTLPWhereExtendedOracle(globalState, OxlaCommon.ALL_ERRORS);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            List<TestOracle<OxlaGlobalState>> oracles = new ArrayList<>();
            oracles.add(AGGREGATE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            // FIXME Cannot test HAVING oracle, because SQLancer itself generates incorrect testing clauses;
            //       They ignore possible duplicate values and trigger false-positive cardinality errors.
            //       oracles.add(HAVING.create(globalState));
            oracles.add(WHERE.create(globalState));
            // FIXME Cannot test WHERE_EXTENDED oracle, because SQLancer itself generates incorrect testing clauses;
            //       They ignore possible duplicate values and trigger false-positive cardinality errors.
            //       oracles.add(WHERE_EXTENDED.create(globalState));
            return new CompositeTestOracle<>(oracles, globalState);
        }

    },
    FUZZER {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaFuzzer(globalState);
        }
    }
}
