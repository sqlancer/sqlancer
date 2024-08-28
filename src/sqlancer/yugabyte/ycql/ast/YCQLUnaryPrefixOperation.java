package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;

public class YCQLUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<YCQLExpression> implements YCQLExpression {
    public YCQLUnaryPrefixOperation(YCQLExpression expr, BinaryOperatorNode.Operator operator) {
        super(expr, operator);
    }
}
