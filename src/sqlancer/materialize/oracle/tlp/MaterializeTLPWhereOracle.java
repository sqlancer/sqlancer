package sqlancer.materialize.oracle.tlp;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeJoin;
import sqlancer.materialize.ast.MaterializeSelect;
import sqlancer.materialize.gen.MaterializeCommon;
import sqlancer.materialize.gen.MaterializeExpressionGenerator;

public class MaterializeTLPWhereOracle implements TestOracle<MaterializeGlobalState> {

    private final TLPWhereOracle<MaterializeSelect, MaterializeJoin, MaterializeExpression, MaterializeSchema, MaterializeTable, MaterializeColumn, MaterializeGlobalState> oracle;

    public MaterializeTLPWhereOracle(MaterializeGlobalState state) {
        MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(MaterializeCommon.getCommonExpressionErrors())
                .with(MaterializeCommon.getCommonFetchErrors()).build();

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
    public Reproducer<MaterializeGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
