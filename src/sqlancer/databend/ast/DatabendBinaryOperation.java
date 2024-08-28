package sqlancer.databend.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class DatabendBinaryOperation extends NewBinaryOperatorNode<DatabendExpression> implements DatabendExpression {
    public DatabendBinaryOperation(DatabendExpression left, DatabendExpression right,
            BinaryOperatorNode.Operator operator) {
        super(left, right, operator);
    }

}
