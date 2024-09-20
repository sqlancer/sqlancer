package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

public class CockroachDBTLPWhereOracle implements TestOracle<CockroachDBGlobalState> {

    private final TLPWhereOracle<CockroachDBSelect, CockroachDBJoin, CockroachDBExpression, CockroachDBSchema, CockroachDBTable, CockroachDBColumn, CockroachDBGlobalState> oracle;

    public CockroachDBTLPWhereOracle(CockroachDBGlobalState state) {
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(CockroachDBErrors.getExpressionErrors())
                .with("GROUP BY term out of range").build();

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
    public Reproducer<CockroachDBGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
