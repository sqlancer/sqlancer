package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.oxla.OxlaToStringVisitor;

public class OxlaBetweenOperator extends NewBetweenOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaBetweenOperator(OxlaExpression left, OxlaExpression middle, OxlaExpression right, boolean isTrue) {
        super(left, middle, right, isTrue);
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
