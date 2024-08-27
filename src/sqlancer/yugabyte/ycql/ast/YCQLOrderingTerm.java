package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;

public class YCQLOrderingTerm extends NewOrderingTerm<YCQLExpression> implements YCQLExpression {
    public YCQLOrderingTerm(YCQLExpression expr, Ordering ordering) {
        super(expr, ordering);
    }
}
