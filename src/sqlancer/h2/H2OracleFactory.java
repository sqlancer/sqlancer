package sqlancer.h2;

import java.sql.SQLException;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

public enum H2OracleFactory implements OracleFactory<H2Provider.H2GlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle<H2Provider.H2GlobalState> create(H2Provider.H2GlobalState globalState) throws SQLException {
            H2ExpressionGenerator gen = new H2ExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(H2Errors.getExpressionErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }

    };

}
