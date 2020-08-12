package sqlancer.common.ast.newast;

public class NewPostfixTextNode<T> implements Node<T> {

    private final Node<T> expr;
    private final String text;

    public NewPostfixTextNode(Node<T> expr, String text) {
        this.expr = expr;
        this.text = text;
    }

    public Node<T> getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }
}
