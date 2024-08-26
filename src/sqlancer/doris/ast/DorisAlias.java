package sqlancer.doris.ast;

import sqlancer.common.ast.newast.NewAliasNode;

public class DorisAlias extends NewAliasNode<DorisExpression> implements DorisExpression {
    public DorisAlias(DorisExpression expr, String text) {
        super(expr, text);
    }
}
