package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewPostfixTextNode;

public class OxlaPostfixText extends NewPostfixTextNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaPostfixText(OxlaExpression expr, String text) {
        super(expr, text);
    }
}
