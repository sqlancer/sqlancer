package sqlancer.mongodb.ast;

import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBBinaryLogicalOperator;

public class MongoDBBinaryLogicalNode extends NewBinaryOperatorNode<MongoDBExpression> {
    public MongoDBBinaryLogicalNode(Node<MongoDBExpression> left, Node<MongoDBExpression> right,
            MongoDBBinaryLogicalOperator op) {
        super(left, right, op);
    }

    public MongoDBBinaryLogicalOperator operator() {
        return (MongoDBBinaryLogicalOperator) op;
    }
}
