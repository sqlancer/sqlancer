package sqlancer.duckdb.test;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;

public class DuckDBNoRECOracle implements TestOracle<DuckDBGlobalState> {

    NoRECOracle<DuckDBSelect, DuckDBJoin, DuckDBExpression, DuckDBSchema, DuckDBTable, DuckDBColumn, DuckDBGlobalState> oracle;

    public DuckDBNoRECOracle(DuckDBGlobalState globalState) {
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(DuckDBErrors.getExpressionErrors())
                .with("canceling statement due to statement timeout").build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<DuckDBGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
