package sqlancer.doris.oracle.tlp;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.gen.DorisNewExpressionGenerator;

public class DorisQueryPartitioningWhereTester implements TestOracle<DorisGlobalState> {

    private final TLPWhereOracle<DorisSelect, DorisJoin, DorisExpression, DorisSchema, DorisTable, DorisColumn, DorisGlobalState> oracle;

    public DorisQueryPartitioningWhereTester(DorisGlobalState state) {
        DorisNewExpressionGenerator gen = new DorisNewExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(DorisErrors.getExpressionErrors())
                .with(DorisErrors.getExpressionErrors()).build();

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
    public Reproducer<DorisGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
