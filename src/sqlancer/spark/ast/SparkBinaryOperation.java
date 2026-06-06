package sqlancer.spark.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class SparkBinaryOperation extends NewBinaryOperatorNode<SparkExpression> implements SparkExpression {

    public SparkBinaryOperation(SparkExpression left, SparkExpression right, Operator op) {
        super(left, right, op);
    }
}
