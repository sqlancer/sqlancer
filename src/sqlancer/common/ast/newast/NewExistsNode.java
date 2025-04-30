package sqlancer.common.ast.newast;

public class NewExistsNode<T> {
    private final T expr;
    private final Boolean isNot;

    public NewExistsNode(T expr, Boolean isNot) {
        this.expr = expr;
        this.isNot = isNot;
    }

    public T getExpr() {
        return expr;
    }

    public Boolean getIsNot() {
        return isNot;
    }
}
