package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewAliasNode;

public class OxlaAlias extends NewAliasNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaAlias(OxlaExpression expr, String alias) {
        super(expr, alias);
    }
}
