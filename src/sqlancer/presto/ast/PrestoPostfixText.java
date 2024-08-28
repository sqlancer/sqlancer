package sqlancer.presto.ast;

import sqlancer.common.ast.newast.NewPostfixTextNode;

public class PrestoPostfixText extends NewPostfixTextNode<PrestoExpression> implements PrestoExpression {
    public PrestoPostfixText(PrestoExpression expr, String text) {
        super(expr, text);
    }
}
