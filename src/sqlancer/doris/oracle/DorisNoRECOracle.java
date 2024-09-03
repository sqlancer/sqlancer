package sqlancer.doris.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
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

public class DorisNoRECOracle implements TestOracle<DorisGlobalState> {

    NoRECOracle<DorisSelect, DorisJoin, DorisExpression, DorisSchema, DorisTable, DorisColumn, DorisGlobalState> oracle;

    public DorisNoRECOracle(DorisGlobalState globalState) {
        DorisNewExpressionGenerator gen = new DorisNewExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(DorisErrors.getExpressionErrors())
                .with("canceling statement due to statement timeout").build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<DorisGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
