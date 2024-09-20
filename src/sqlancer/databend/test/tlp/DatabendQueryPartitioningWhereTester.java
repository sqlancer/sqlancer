package sqlancer.databend.test.tlp;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.gen.DatabendNewExpressionGenerator;

public class DatabendQueryPartitioningWhereTester implements TestOracle<DatabendGlobalState> {

    private final TLPWhereOracle<DatabendSelect, DatabendJoin, DatabendExpression, DatabendSchema, DatabendTable, DatabendColumn, DatabendGlobalState> oracle;

    public DatabendQueryPartitioningWhereTester(DatabendGlobalState state) {
        DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(DatabendErrors.getExpressionErrors())
                .with(DatabendErrors.getGroupByErrors()).build();

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
    public Reproducer<DatabendGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
