package sqlancer.doris.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class DorisBinaryOperation extends NewBinaryOperatorNode<DorisExpression> implements DorisExpression {
    public DorisBinaryOperation(DorisExpression left, DorisExpression right, BinaryOperatorNode.Operator op) {
        super(left, right, op);
    }
}
