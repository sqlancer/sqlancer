package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.oxla.OxlaToStringVisitor;

public class OxlaOrderingTerm extends NewOrderingTerm<OxlaExpression>
        implements OxlaExpression {
    public OxlaOrderingTerm(OxlaExpression expr, Ordering ordering) {
        super(expr, ordering);
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
