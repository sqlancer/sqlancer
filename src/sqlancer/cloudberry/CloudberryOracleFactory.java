package sqlancer.cloudberry;

import java.sql.SQLException;

import sqlancer.OracleFactory;
import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.PostgresFuzzer;

public enum CloudberryOracleFactory implements OracleFactory<PostgresGlobalState> {
    NOREC {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                    .with(PostgresCommon.getCommonFetchErrors())
                    .withRegex(PostgresCommon.getCommonExpressionRegexErrors())
                    .with(CloudberryCommon.getCloudberryErrors()).build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    WHERE {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
            PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                    .with(PostgresCommon.getCommonFetchErrors())
                    .withRegex(PostgresCommon.getCommonExpressionRegexErrors())
                    .with(CloudberryCommon.getCloudberryErrors()).build();
            return new TLPWhereOracle<>(globalState, gen, errors);
        }
    },
    // PostgresFuzzer ignores every SQLException, so it needs no Cloudberry expected errors.
    FUZZER {
        @Override
        public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws Exception {
            return new PostgresFuzzer(globalState);
        }
    };

}
