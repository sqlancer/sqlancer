package sqlancer.hsqldb;

import java.sql.SQLException;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.hsqldb.gen.HSQLDBExpressionGenerator;

public enum HSQLDBOracleFactory implements OracleFactory<HSQLDBProvider.HSQLDBGlobalState> {
    WHERE {
        @Override
        public TestOracle<HSQLDBProvider.HSQLDBGlobalState> create(HSQLDBProvider.HSQLDBGlobalState globalState)
                throws SQLException {
            HSQLDBExpressionGenerator gen = new HSQLDBExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(HSQLDBErrors.getExpressionErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    NOREC {
        @Override
        public TestOracle<HSQLDBProvider.HSQLDBGlobalState> create(HSQLDBProvider.HSQLDBGlobalState globalState)
                throws Exception {
            HSQLDBExpressionGenerator gen = new HSQLDBExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(HSQLDBErrors.getExpressionErrors()).build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    }
}
