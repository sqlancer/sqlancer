package sqlancer.mysql;

import java.sql.SQLException;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.mysql.oracle.MySQLCERTOracle;
import sqlancer.mysql.oracle.MySQLDQPOracle;
import sqlancer.mysql.oracle.MySQLFuzzer;
import sqlancer.mysql.oracle.MySQLPivotedQuerySynthesisOracle;

public enum MySQLOracleFactory implements OracleFactory<MySQLGlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(MySQLErrors.getExpressionErrors())
                    .withRegex(MySQLErrors.getExpressionRegexErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }

    },
    PQS {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            return new MySQLPivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }

    },
    CERT {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            return new MySQLCERTOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    FUZZER {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws Exception {
            return new MySQLFuzzer(globalState);
        }

    },
    DQP {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            return new MySQLDQPOracle(globalState);
        }
    };
}
