package sqlancer;

/**
 * This is the base class of the Ternary Logic Partitioning (TLP) oracles. The core idea of TLP is to partition a given
 * so-called original query to three so-called partitioning queries, each of which computes a partition of the original
 * query's result.
 *
 * @param <E>
 *            the expression type
 */
public abstract class TernaryLogicPartitioningOracleBase<E> {

    protected E predicate;
    protected E negatedPredicate;
    protected E isNullPredicate;

    protected TernaryLogicPartitioningOracleBase() {
    }

}
