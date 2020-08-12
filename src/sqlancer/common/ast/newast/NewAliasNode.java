package sqlancer.common.ast.newast;

public class NewAliasNode<E> implements Node<E> {

    private final Node<E> expr;
    private final String alias;

    public NewAliasNode(Node<E> expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    public Node<E> getExpr() {
        return expr;
    }

    public String getAlias() {
        return alias;
    }

}
