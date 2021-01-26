package sqlancer.mongodb.ast;

import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBUnaryLogicalOperator;

public class MongoDBUnaryLogicalOperatorNode extends NewUnaryPrefixOperatorNode<MongoDBExpression> {

    public MongoDBUnaryLogicalOperatorNode(Node<MongoDBExpression> expr, MongoDBUnaryLogicalOperator op) {
        super(expr, op);
    }

    public MongoDBUnaryLogicalOperator operator() {
        return (MongoDBUnaryLogicalOperator) op;
    }
}
