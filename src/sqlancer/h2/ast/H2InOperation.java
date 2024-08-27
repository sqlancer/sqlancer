package sqlancer.h2.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewInOperatorNode;

public class H2InOperation extends NewInOperatorNode<H2Expression> implements H2Expression {
    public H2InOperation(H2Expression left, List<H2Expression> right, boolean isNegated) {
        super(left, right, isNegated);
    }
}
