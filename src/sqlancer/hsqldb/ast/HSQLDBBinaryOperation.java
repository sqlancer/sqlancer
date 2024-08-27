package sqlancer.hsqldb.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class HSQLDBBinaryOperation extends NewBinaryOperatorNode<HSQLDBExpression> implements HSQLDBExpression {
    public HSQLDBBinaryOperation(HSQLDBExpression left, HSQLDBExpression right, Operator op) {
        super(left, right, op);
    }
}
