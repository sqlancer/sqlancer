package sqlancer.oceanbase.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oceanbase.OceanBaseErrors;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.ast.OceanBaseExpression;
import sqlancer.oceanbase.ast.OceanBaseJoin;
import sqlancer.oceanbase.ast.OceanBaseSelect;
import sqlancer.oceanbase.gen.OceanBaseExpressionGenerator;

public class OceanBaseTLPWhereOracle implements TestOracle<OceanBaseGlobalState> {

    private final TLPWhereOracle<OceanBaseSelect, OceanBaseJoin, OceanBaseExpression, OceanBaseSchema, OceanBaseTable, OceanBaseColumn, OceanBaseGlobalState> oracle;

    public OceanBaseTLPWhereOracle(OceanBaseGlobalState state) {
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(OceanBaseErrors.getExpressionErrors())
                .withRegex(OceanBaseErrors.getExpressionErrorsRegex()).with("value is out of range").build();

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
    public Reproducer<OceanBaseGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
