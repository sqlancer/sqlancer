package sqlancer.citus.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.common.oracle.NoRECOracle;
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

public class CitusNoRECOracle implements TestOracle<PostgresGlobalState> {
    private final NoRECOracle<PostgresSelect, PostgresJoin, PostgresExpression, PostgresSchema, PostgresTable, PostgresColumn, PostgresGlobalState> oracle;

    public CitusNoRECOracle(PostgresGlobalState globalState) {
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(PostgresCommon.getCommonExpressionErrors())
                .with(PostgresCommon.getCommonFetchErrors()).withRegex(PostgresCommon.getCommonExpressionRegexErrors())
                .with(CitusCommon.getCitusErrors().toArray(new String[0])).build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }

    @Override
    public Reproducer<PostgresGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

}
