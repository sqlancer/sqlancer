package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;

import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresTLPWhereOracle implements TestOracle<PostgresGlobalState> {

    private final TLPWhereOracle<PostgresSelect, PostgresJoin, PostgresExpression, PostgresSchema, PostgresTable, PostgresColumn, PostgresGlobalState> oracle;

    public PostgresTLPWhereOracle(PostgresGlobalState state) {
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                .with(PostgresCommon.getCommonFetchErrors()).withRegex(PostgresCommon.getCommonExpressionRegexErrors())
                .build();

        this.oracle = new TLPWhereOracle<>(state, gen, expectedErrors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
