package sqlancer.databend.ast;

import sqlancer.common.ast.newast.NewAliasNode;

public class DatabendAlias extends NewAliasNode<DatabendExpression> implements DatabendExpression {
    public DatabendAlias(DatabendExpression expr, String text) {
        super(expr, text);
    }
}
