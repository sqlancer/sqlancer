package sqlancer.oceanbase;

import java.sql.SQLException;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oceanbase.gen.OceanBaseExpressionGenerator;
import sqlancer.oceanbase.oracle.OceanBasePivotedQuerySynthesisOracle;

public enum OceanBaseOracleFactory implements OracleFactory<OceanBaseGlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle<OceanBaseGlobalState> create(OceanBaseGlobalState globalState) throws SQLException {
            OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(OceanBaseErrors.getExpressionErrors())
                    .withRegex(OceanBaseErrors.getExpressionErrorsRegex()).with("value is out of range").build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    NoREC {
        @Override
        public TestOracle<OceanBaseGlobalState> create(OceanBaseGlobalState globalState) throws SQLException {
            OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(OceanBaseErrors.getExpressionErrors())
                    .withRegex(OceanBaseErrors.getExpressionErrorsRegex())
                    .with("canceling statement due to statement timeout").with("unmatched parentheses")
                    .with("nothing to repeat at offset").with("missing )").with("missing terminating ]")
                    .with("range out of order in character class").with("unrecognized character after ")
                    .with("Got error '(*VERB) not recognized or malformed").with("must be followed by")
                    .with("malformed number or name after").with("digit expected after").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    PQS {
        @Override
        public TestOracle<OceanBaseGlobalState> create(OceanBaseGlobalState globalState) throws SQLException {
            return new OceanBasePivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    }
}
