package sqlancer.presto.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class PrestoBinaryOperation extends NewBinaryOperatorNode<PrestoExpression> implements PrestoExpression {
    public PrestoBinaryOperation(PrestoExpression left, PrestoExpression right, BinaryOperatorNode.Operator op) {
        super(left, right, op);
    }
}
