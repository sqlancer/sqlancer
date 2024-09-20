package sqlancer.clickhouse.oracle.tlp;

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
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

public class ClickHouseTLPWhereOracle implements TestOracle<ClickHouseGlobalState> {

    private final TLPWhereOracle<ClickHouseSelect, ClickHouseJoin, ClickHouseExpression, ClickHouseSchema, ClickHouseTable, ClickHouseColumn, ClickHouseGlobalState> oracle;

    public ClickHouseTLPWhereOracle(ClickHouseGlobalState state) {
        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(ClickHouseErrors.getExpectedExpressionErrors())
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

    @Override
    public Reproducer<ClickHouseGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
