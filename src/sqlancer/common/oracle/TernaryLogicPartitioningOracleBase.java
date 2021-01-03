package sqlancer.common.oracle;

import sqlancer.GlobalState;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.query.ExpectedErrors;

/**
 * This is the base class of the Ternary Logic Partitioning (TLP) oracles. The core idea of TLP is to partition a given
 * so-called original query to three so-called partitioning queries, each of which computes a partition of the original
 * query's result.
 *
 * @param <E>
 *            the expression type
 * @param <S>
 *            the global state type
 */
public abstract class TernaryLogicPartitioningOracleBase<E, S extends GlobalState<?, ?, ?>> implements TestOracle {

    protected E predicate;
    protected E negatedPredicate;
    protected E isNullPredicate;

    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();

    protected TernaryLogicPartitioningOracleBase(S state) {
        this.state = state;
    }

    protected E generatePredicate() {
        return getGen().generatePredicate();
    }

    protected void initializeTernaryPredicateVariants() {
        ExpressionGenerator<E> gen = getGen();
        if (gen == null) {
            throw new IllegalStateException();
        }
        predicate = generatePredicate();
        if (predicate == null) {
            throw new IllegalStateException();
        }
        negatedPredicate = gen.negatePredicate(predicate);
        if (negatedPredicate == null) {
            throw new IllegalStateException();
        }
        isNullPredicate = gen.isNull(predicate);
        if (isNullPredicate == null) {
            throw new IllegalStateException();
        }
    }

    protected abstract ExpressionGenerator<E> getGen();

}
