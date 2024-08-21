package sqlancer.common.ast.newast;

import sqlancer.Randomly;

public class NewOrderingTerm<T> implements Node<T> {

    private final T expr;
    private final Ordering ordering;

    public enum Ordering {
        ASC, DESC;

        public static Ordering getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public NewOrderingTerm(T expr, Ordering ordering) {
        this.expr = expr;
        this.ordering = ordering;
    }

    public T getExpr() {
        return expr;
    }

    public Ordering getOrdering() {
        return ordering;
    }

}
