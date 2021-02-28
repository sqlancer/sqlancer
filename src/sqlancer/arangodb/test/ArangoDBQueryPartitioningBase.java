package sqlancer.arangodb.test;

import sqlancer.arangodb.ArangoDBProvider;
import sqlancer.arangodb.ast.ArangoDBExpression;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;

public class ArangoDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<ArangoDBExpression>, ArangoDBProvider.ArangoDBGlobalState>
        implements TestOracle {

    protected ArangoDBQueryPartitioningBase(ArangoDBProvider.ArangoDBGlobalState state) {
        super(state);
    }

    @Override
    protected ExpressionGenerator<Node<ArangoDBExpression>> getGen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void check() throws Exception {

    }
}
