package sqlancer.doris.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;

public class DorisOrderByTerm extends NewOrderingTerm<DorisExpression> implements DorisExpression {
    public DorisOrderByTerm(DorisExpression expr, Ordering ordering) {
        super(expr, ordering);
    }
}
