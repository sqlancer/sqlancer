package sqlancer.datafusion.test;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionJoin;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

public class DataFusionNoRECOracle implements TestOracle<DataFusionGlobalState> {

    NoRECOracle<DataFusionSelect, DataFusionJoin, DataFusionExpression, DataFusionSchema, DataFusionTable, DataFusionColumn, DataFusionGlobalState> oracle;

    public DataFusionNoRECOracle(DataFusionGlobalState globalState) {
        DataFusionExpressionGenerator gen = new DataFusionExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(DataFusionErrors.getExpectedExecutionErrors())
                .with("canceling statement due to statement timeout").build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<DataFusionGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
