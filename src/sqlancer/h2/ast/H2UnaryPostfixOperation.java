package sqlancer.h2.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class H2UnaryPostfixOperation extends NewUnaryPostfixOperatorNode<H2Expression> implements H2Expression {
    public H2UnaryPostfixOperation(H2Expression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }
}
