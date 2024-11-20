package sqlancer.common.ast.newast;

public abstract class NewGroupByTerm<E> {

    protected final E expr;

    public NewGroupByTerm(E expr) {
        this.expr = expr;
    }

    public E getExpression() {
        return expr;
    }

    public abstract String asString();
}