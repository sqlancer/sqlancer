package sqlancer.common.ast.newast;

import java.util.List;

public class NewWithNode<T> {
    private final T leftExpr;
    private final List<T> rightExpr;

    public NewWithNode(T leftExpr, List<T> rightExpr) {
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
    }

    public T getLeftExpr() {
        return this.leftExpr;
    }

    public List<T> getRightExpr() {
        return this.rightExpr;
    }
}
