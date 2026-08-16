package sqlancer.h2;

import java.sql.SQLException;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.TLPWhereOracle2;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.h2.dialect.H2Dialect;

public enum H2OracleFactory implements OracleFactory<H2Provider.H2GlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle<H2Provider.H2GlobalState> create(H2Provider.H2GlobalState globalState) throws SQLException {
            H2Dialect dialect = new H2Dialect();
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(H2Errors.getExpressionErrors())
                    .withRegexString("Column \".*\" not found").build();

            return new TLPWhereOracle2<>(globalState, dialect, expectedErrors);
        }

    };

}
