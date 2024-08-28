package sqlancer.datafusion.test;

import java.sql.SQLException;

import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

public class DataFusionQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<DataFusionExpression, DataFusionGlobalState>
        implements TestOracle<DataFusionGlobalState> {
    DataFusionGlobalState state;
    DataFusionExpressionGenerator gen;
    DataFusionSelect select;

    public DataFusionQueryPartitioningBase(DataFusionGlobalState state) {
        super(state);
        this.state = state;
    }

    @Override
    public void check() throws SQLException {
        select = DataFusionSelect.getRandomSelect(state);
        gen = select.exprGen;
        initializeTernaryPredicateVariants();
    }

    @Override
    protected ExpressionGenerator<DataFusionExpression> getGen() {
        return gen;
    }

}
