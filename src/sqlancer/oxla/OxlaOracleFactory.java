package sqlancer.oxla;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oxla.gen.OxlaExpressionGenerator;
import sqlancer.oxla.oracle.OxlaFuzzer;
import sqlancer.oxla.oracle.OxlaPivotedQuerySynthesisOracle;

import java.util.ArrayList;
import java.util.List;

public enum OxlaOracleFactory implements OracleFactory<OxlaGlobalState> {
    NOREC {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            OxlaExpressionGenerator generator = new OxlaExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors()
                    .with(OxlaCommon.SYNTAX_ERRORS)
                    .withRegex(OxlaCommon.SYNTAX_REGEX_ERRORS)
                    .with(OxlaCommon.JOIN_ERRORS)
                    .with(OxlaCommon.GROUP_BY_ERRORS)
                    .withRegex(OxlaCommon.GROUP_BY_REGEX_ERRORS)
                    .with(OxlaCommon.ORDER_BY_ERRORS)
                    .withRegex(OxlaCommon.ORDER_BY_REGEX_ERRORS)
                    .with(OxlaCommon.EXPRESSION_ERRORS)
                    .withRegex(OxlaCommon.EXPRESSION_REGEX_ERRORS)
                    .with(OxlaCommon.bugErrors())
                    .build();
            return new NoRECOracle<>(globalState, generator, errors);
        }
    },
    PQS {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaPivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            List<TestOracle<OxlaGlobalState>> oracles = new ArrayList<>();
            // TODO
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
