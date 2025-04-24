package sqlancer.oxla.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class OxlaBinaryOperation extends NewBinaryOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaBinaryOperation(OxlaExpression left, OxlaExpression right, BinaryOperatorNode.Operator op) {
        super(left, right, op);
    }
}
