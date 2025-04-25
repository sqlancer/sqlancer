package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewTernaryNode;

public class OxlaTernaryNode extends NewTernaryNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaTernaryNode(OxlaExpression left,
                           OxlaExpression middle,
                           OxlaExpression right,
                           String leftStr,
                           String rightStr) {
        super(left, middle, right, leftStr, rightStr);
    }
}
