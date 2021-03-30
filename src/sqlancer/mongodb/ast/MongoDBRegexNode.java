package sqlancer.mongodb.ast;

import static sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBRegexOperator.REGEX;

import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.mongodb.gen.MongoDBMatchExpressionGenerator.MongoDBRegexOperator;

public class MongoDBRegexNode extends NewBinaryOperatorNode<MongoDBExpression> {
    private final String options;

    public MongoDBRegexNode(Node<MongoDBExpression> left, Node<MongoDBExpression> right, String options) {
        super(left, right, REGEX);
        this.options = options;
    }

    public String getOptions() {
        return options;
    }

    public MongoDBRegexOperator operator() {
        return (MongoDBRegexOperator) op;
    }
}
