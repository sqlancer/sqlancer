package sqlancer.mongodb.test;

import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState;
import sqlancer.mongodb.ast.MongoDBExpression;

public class MongoDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<MongoDBExpression>, MongoDBGlobalState> implements TestOracle {

    public MongoDBQueryPartitioningBase(MongoDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {

    }

    @Override
    protected ExpressionGenerator<Node<MongoDBExpression>> getGen() {
        throw new UnsupportedOperationException();
    }
}
