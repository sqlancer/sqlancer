package sqlancer.materialize.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
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

public class MaterializeNoRECOracle implements TestOracle<MaterializeGlobalState> {

    NoRECOracle<MaterializeSelect, MaterializeJoin, MaterializeExpression, MaterializeSchema, MaterializeTable, MaterializeColumn, MaterializeGlobalState> oracle;

    public MaterializeNoRECOracle(MaterializeGlobalState globalState) {
        MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(MaterializeCommon.getCommonExpressionErrors())
                .with(MaterializeCommon.getCommonFetchErrors()).with("canceling statement due to statement timeout")
                .build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<MaterializeGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
