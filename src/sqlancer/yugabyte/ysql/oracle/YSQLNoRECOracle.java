package sqlancer.yugabyte.ysql.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
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

public class YSQLNoRECOracle implements TestOracle<YSQLGlobalState> {

    NoRECOracle<YSQLSelect, YSQLJoin, YSQLExpression, YSQLSchema, YSQLTable, YSQLColumn, YSQLGlobalState> oracle;

    public YSQLNoRECOracle(YSQLGlobalState globalState) {
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(YSQLErrors.getCommonExpressionErrors())
                .with(YSQLErrors.getCommonFetchErrors()).with("canceling statement due to statement timeout").build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<YSQLGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
