package sqlancer.tidb.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBSelect;

public class TiDBTLPWhereOracle implements TestOracle<TiDBGlobalState> {

    private final TLPWhereOracle<TiDBSelect, TiDBJoin, TiDBExpression, TiDBSchema, TiDBTable, TiDBColumn, TiDBGlobalState> oracle;

    public TiDBTLPWhereOracle(TiDBGlobalState state) {
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(TiDBErrors.getExpressionErrors()).build();

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
    public Reproducer<TiDBGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
