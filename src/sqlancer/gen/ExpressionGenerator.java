package sqlancer.gen;

public interface ExpressionGenerator<E> {

    /**
     * Generates a boolean predicate.
     *
     * @return an expression that can be used in a boolean context.
     */
    E generatePredicate();

}
