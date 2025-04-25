package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class OxlaBetweenOperator extends NewBetweenOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaBetweenOperator(OxlaExpression left, OxlaExpression middle, OxlaExpression right, boolean isTrue) {
        super(left, middle, right, isTrue);
    }
}
