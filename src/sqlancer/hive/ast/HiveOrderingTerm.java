package sqlancer.hive.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;

public class HiveOrderingTerm extends NewOrderingTerm<HiveExpression> implements HiveExpression {

    public HiveOrderingTerm(HiveExpression expr, Ordering ordering) {
        super(expr, ordering);
    }
}
