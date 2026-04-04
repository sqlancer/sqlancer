package sqlancer.spark.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class SparkUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<SparkExpression>
        implements SparkExpression {

    public SparkUnaryPostfixOperation(SparkExpression expr, Operator op) {
        super(expr, op);
    }

}
