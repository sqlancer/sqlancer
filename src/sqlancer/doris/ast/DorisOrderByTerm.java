package sqlancer.doris.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.Node;

public class DorisOrderByTerm extends NewOrderingTerm<DorisExpression> implements DorisExpression {
    public DorisOrderByTerm(Node<DorisExpression> expr, Ordering ordering) {
        super(expr, ordering);
    }
}
