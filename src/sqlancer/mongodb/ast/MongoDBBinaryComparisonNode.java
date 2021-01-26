package sqlancer.mongodb.ast;

import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBBinaryComparisonOperator;

public class MongoDBBinaryComparisonNode extends NewBinaryOperatorNode<MongoDBExpression> {
    public MongoDBBinaryComparisonNode(Node<MongoDBExpression> left, Node<MongoDBExpression> right,
            MongoDBBinaryComparisonOperator op) {
        super(left, right, op);
    }

    public MongoDBBinaryComparisonOperator operator() {
        return (MongoDBBinaryComparisonOperator) op;
    }
}
