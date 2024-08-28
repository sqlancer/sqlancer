package sqlancer.databend.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;

public class DatabendOrderByTerm extends NewOrderingTerm<DatabendExpression> implements DatabendExpression {
    public DatabendOrderByTerm(DatabendExpression expr, Ordering ordering) {
        super(expr, ordering);
    }
}
