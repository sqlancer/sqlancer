package sqlancer.mongodb.ast;

import static sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBRegexOperator.REGEX;

import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBRegexOperator;

public class MongoDBRegexNode extends NewBinaryOperatorNode<MongoDBExpression> {
    public MongoDBRegexNode(Node<MongoDBExpression> left, Node<MongoDBExpression> right) {
        super(left, right, REGEX);
    }

    public MongoDBRegexOperator operator() {
        return (MongoDBRegexOperator) op;
    }
}
