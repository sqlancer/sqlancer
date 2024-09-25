package sqlancer.yugabyte.ysql.oracle.tlp;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLJoin;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;

public class YSQLTLPWhereOracle implements TestOracle<YSQLGlobalState> {

    private final TLPWhereOracle<YSQLSelect, YSQLJoin, YSQLExpression, YSQLSchema, YSQLTable, YSQLColumn, YSQLGlobalState> oracle;

    public YSQLTLPWhereOracle(YSQLGlobalState state) {
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(YSQLErrors.getCommonExpressionErrors())
                .with(YSQLErrors.getCommonFetchErrors()).build();

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
    public Reproducer<YSQLGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
