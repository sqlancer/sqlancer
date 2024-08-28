package sqlancer.h2.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class H2BetweenOperation extends NewBetweenOperatorNode<H2Expression> implements H2Expression {
    public H2BetweenOperation(H2Expression left, H2Expression middle, H2Expression right, boolean isTrue) {
        super(left, middle, right, isTrue);
    }
}
