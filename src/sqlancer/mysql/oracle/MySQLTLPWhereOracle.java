package sqlancer.mysql.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public class MySQLTLPWhereOracle implements TestOracle<MySQLGlobalState> {

    private final TLPWhereOracle<MySQLSelect, MySQLJoin, MySQLExpression, MySQLSchema, MySQLTable, MySQLColumn, MySQLGlobalState> oracle;

    public MySQLTLPWhereOracle(MySQLGlobalState state) {
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(MySQLErrors.getExpressionErrors())
                .withRegex(MySQLErrors.getExpressionRegexErrors()).build();

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
    public Reproducer<MySQLGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
