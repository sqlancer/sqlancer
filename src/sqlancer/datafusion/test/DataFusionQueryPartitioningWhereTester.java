package sqlancer.datafusion.test;

import java.sql.SQLException;

import sqlancer.common.oracle.TLPWhereOracle;
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

public class DataFusionQueryPartitioningWhereTester implements TestOracle<DataFusionGlobalState> {

    private final TLPWhereOracle<DataFusionSelect, DataFusionJoin, DataFusionExpression, DataFusionSchema, DataFusionTable, DataFusionColumn, DataFusionGlobalState> oracle;

    public DataFusionQueryPartitioningWhereTester(DataFusionGlobalState state) {
        DataFusionExpressionGenerator gen = new DataFusionExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(DataFusionErrors.getExpectedExecutionErrors())
                .build();

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
}
