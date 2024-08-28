package sqlancer.databend.ast;

import sqlancer.common.ast.newast.NewPostfixTextNode;

public class DatabendPostFixText extends NewPostfixTextNode<DatabendExpression> implements DatabendExpression {
    public DatabendPostFixText(DatabendExpression expr, String text) {
        super(expr, text);
    }
}
