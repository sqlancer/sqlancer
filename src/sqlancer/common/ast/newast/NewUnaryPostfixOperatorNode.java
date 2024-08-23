package sqlancer.common.ast.newast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class NewUnaryPostfixOperatorNode<T> {

    protected final Operator op;
    private final T expr;

    public NewUnaryPostfixOperatorNode(T expr, Operator op) {
        this.expr = expr;
        this.op = op;
    }

    public String getOperatorRepresentation() {
        return op.getTextRepresentation();
    }

    public T getExpr() {
        return expr;
    }
}
