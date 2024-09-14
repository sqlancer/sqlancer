package sqlancer.presto.test;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
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

public class PrestoNoRECOracle implements TestOracle<PrestoGlobalState> {

    NoRECOracle<PrestoSelect, PrestoJoin, PrestoExpression, PrestoSchema, PrestoTable, PrestoColumn, PrestoGlobalState> oracle;

    public PrestoNoRECOracle(PrestoGlobalState globalState) {
        PrestoTypedExpressionGenerator gen = new PrestoTypedExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(PrestoErrors.getExpressionErrors())
                .with("canceling statement due to statement timeout").build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<PrestoGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
