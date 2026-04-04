package sqlancer.spark.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;

public class SparkUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<SparkExpression> implements SparkExpression {

    public SparkUnaryPrefixOperation(SparkExpression expr, Operator op) {
        super(expr, op);
    }

}
