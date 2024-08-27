package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class YCQLUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<YCQLExpression> implements YCQLExpression {
    public YCQLUnaryPostfixOperation(YCQLExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }
}
