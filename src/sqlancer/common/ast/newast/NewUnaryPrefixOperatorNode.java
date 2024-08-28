package sqlancer.common.ast.newast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class NewUnaryPrefixOperatorNode<T> {

    protected final Operator op;
    private final T expr;

    public NewUnaryPrefixOperatorNode(T expr, Operator op) {
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
