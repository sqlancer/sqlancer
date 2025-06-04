package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.oxla.OxlaToStringVisitor;

public class OxlaPostfixText extends NewPostfixTextNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaPostfixText(OxlaExpression expr, String text) {
        super(expr, text);
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
