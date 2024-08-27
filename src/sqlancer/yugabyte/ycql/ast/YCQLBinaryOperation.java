package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class YCQLBinaryOperation extends NewBinaryOperatorNode<YCQLExpression> implements YCQLExpression {
    public YCQLBinaryOperation(YCQLExpression left, YCQLExpression right, Operator op) {
        super(left, right, op);
    }
}
