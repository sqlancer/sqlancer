package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.oxla.OxlaToStringVisitor;

import java.util.List;

public class OxlaInOperator extends NewInOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaInOperator(OxlaExpression left, List<OxlaExpression> right, boolean isNegated) {
        super(left, right, isNegated);
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
