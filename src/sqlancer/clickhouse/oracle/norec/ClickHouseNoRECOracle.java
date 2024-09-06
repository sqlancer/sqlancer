package sqlancer.clickhouse.oracle.norec;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseExpression.ClickHouseJoin;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

public class ClickHouseNoRECOracle implements TestOracle<ClickHouseGlobalState> {

    NoRECOracle<ClickHouseSelect, ClickHouseJoin, ClickHouseExpression, ClickHouseSchema, ClickHouseTable, ClickHouseColumn, ClickHouseGlobalState> oracle;

    public ClickHouseNoRECOracle(ClickHouseGlobalState globalState) {
        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(ClickHouseErrors.getExpectedExpressionErrors())
                .with("canceling statement due to statement timeout").build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<ClickHouseGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }

}
