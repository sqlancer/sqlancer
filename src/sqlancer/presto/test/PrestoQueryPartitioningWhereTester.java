package sqlancer.presto.test;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoJoin;
import sqlancer.presto.ast.PrestoSelect;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public class PrestoQueryPartitioningWhereTester implements TestOracle<PrestoGlobalState> {

    private final TLPWhereOracle<PrestoSelect, PrestoJoin, PrestoExpression, PrestoSchema, PrestoTable, PrestoColumn, PrestoGlobalState> oracle;

    public PrestoQueryPartitioningWhereTester(PrestoGlobalState state) {
        PrestoTypedExpressionGenerator gen = new PrestoTypedExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(PrestoErrors.getExpressionErrors())
                .with(PrestoErrors.getGroupByErrors()).build();

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
    public Reproducer<PrestoGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }
}
