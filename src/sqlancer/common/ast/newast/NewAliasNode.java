package sqlancer.common.ast.newast;

public class NewAliasNode<E> implements Node<E> {

    private final E expr;
    private final String alias;

    public NewAliasNode(E expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    public E getExpr() {
        return expr;
    }

    public String getAlias() {
        return alias;
    }

}
