package sqlancer.duckdb.test;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
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

public class DuckDBQueryPartitioningWhereTester implements TestOracle<DuckDBGlobalState> {

    private final TLPWhereOracle<DuckDBSelect, DuckDBJoin, DuckDBExpression, DuckDBSchema, DuckDBTable, DuckDBColumn, DuckDBGlobalState> oracle;

    public DuckDBQueryPartitioningWhereTester(DuckDBGlobalState state) {
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(DuckDBErrors.getExpressionErrors())
                .with(DuckDBErrors.getGroupByErrors()).withRegex(DuckDBErrors.getExpressionErrorsRegex()).build();

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
    public Reproducer<DuckDBGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
