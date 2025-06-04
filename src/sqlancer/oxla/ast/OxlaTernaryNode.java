package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewTernaryNode;
import sqlancer.oxla.OxlaToStringVisitor;

public class OxlaTernaryNode extends NewTernaryNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaTernaryNode(OxlaExpression left,
                           OxlaExpression middle,
                           OxlaExpression right,
                           String leftStr,
                           String rightStr) {
        super(left, middle, right, leftStr, rightStr);
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
