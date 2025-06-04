package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewAliasNode;
import sqlancer.oxla.OxlaToStringVisitor;

public class OxlaAlias extends NewAliasNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaAlias(OxlaExpression expr, String alias) {
        super(expr, alias);
    }

    @Override
    public OxlaConstant getExpectedValue() {
        return getExpr().getExpectedValue();
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
