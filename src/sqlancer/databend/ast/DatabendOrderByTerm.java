package sqlancer.databend.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.Node;

public class DatabendOrderByTerm extends NewOrderingTerm<DatabendExpression> implements DatabendExpression {
    public DatabendOrderByTerm(Node<DatabendExpression> expr, Ordering ordering) {
        super(expr, ordering);
    }
}
