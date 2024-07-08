package sqlancer.common.gen;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.schema.AbstractTableColumn;

public interface PartitionGenerator<E extends Expression<C>, C extends AbstractTableColumn<?, ?>> {

    /**
     * Negates a predicate (i.e., uses a NOT operator).
     *
     * @param predicate
     *            the boolean predicate.
     *
     * @return the negated predicate.
     */
    E negatePredicate(E predicate);

    /**
     * Checks if an expression evaluates to NULL (i.e., implements the IS NULL operator).
     *
     * @param expr
     *            the expression
     *
     * @return an expression that checks whether the expression evaluates to NULL.
     */
    E isNull(E expr);
}
