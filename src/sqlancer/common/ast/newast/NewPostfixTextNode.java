package sqlancer.common.ast.newast;

public class NewPostfixTextNode<T> {

    private final T expr;
    private final String text;

    public NewPostfixTextNode(T expr, String text) {
        this.expr = expr;
        this.text = text;
    }

    public T getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }
}
