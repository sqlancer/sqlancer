package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;

public class OxlaOrderingTerm extends NewOrderingTerm<OxlaExpression>
        implements OxlaExpression {
    public OxlaOrderingTerm(OxlaExpression expr, Ordering ordering) {
        super(expr, ordering);
    }
}
