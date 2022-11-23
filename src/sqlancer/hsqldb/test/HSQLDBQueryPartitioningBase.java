package sqlancer.hsqldb.test;

import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.ast.HSQLDBExpression;

public class HSQLDBQueryPartitioningBase extends TernaryLogicPartitioningOracleBase<Node<HSQLDBExpression>, HSQLDBProvider.HSQLDBGlobalState> implements TestOracle {

    public HSQLDBQueryPartitioningBase(HSQLDBProvider.HSQLDBGlobalState state) {
        super(state);
    }

    @Override
    protected ExpressionGenerator<Node<HSQLDBExpression>> getGen() {
        return null;
    }

    @Override
    public void check() throws Exception {

    }
}
