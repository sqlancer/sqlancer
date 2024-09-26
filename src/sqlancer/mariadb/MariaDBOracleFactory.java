package sqlancer.mariadb;

import java.sql.SQLException;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;
import sqlancer.mariadb.oracle.MariaDBDQPOracle;

public enum MariaDBOracleFactory implements OracleFactory<MariaDBProvider.MariaDBGlobalState> {

    NOREC {
        @Override
        public TestOracle<MariaDBProvider.MariaDBGlobalState> create(MariaDBProvider.MariaDBGlobalState globalState)
                throws SQLException {
            MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(globalState.getRandomly());
            ExpectedErrors errors = ExpectedErrors.newErrors().with(MariaDBErrors.getCommonErrors())
                    .with("is out of range").with("unmatched parentheses").with("nothing to repeat at offset")
                    .with("missing )").with("missing terminating ]").with("range out of order in character class")
                    .with("unrecognized character after ").with("Got error '(*VERB) not recognized or malformed")
                    .with("must be followed by").with("malformed number or name after").with("digit expected after")
                    .with("Could not create a join buffer").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }

    },
    DQP {
        @Override
        public TestOracle<MariaDBProvider.MariaDBGlobalState> create(MariaDBProvider.MariaDBGlobalState globalState)
                throws SQLException {
            return new MariaDBDQPOracle(globalState);
        }
    }
}
