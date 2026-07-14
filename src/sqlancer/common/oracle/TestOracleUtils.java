package sqlancer.common.oracle;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.gen.PartitionGenerator;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public final class TestOracleUtils {

    private TestOracleUtils() {
    }

    public static final class PredicateVariants<E extends Expression<C>, C extends AbstractTableColumn<?, ?>> {
        public E predicate;
        public E negatedPredicate;
        public E isNullPredicate;

        PredicateVariants(E predicate, E negatedPredicate, E isNullPredicate) {
            this.predicate = predicate;
            this.negatedPredicate = negatedPredicate;
            this.isNullPredicate = isNullPredicate;
        }
    }

    public static <T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> AbstractTables<T, C> getRandomTableNonEmptyTables(
            AbstractSchema<?, T> schema) {
        if (schema.getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        return new AbstractTables<>(Randomly.nonEmptySubset(schema.getDatabaseTables()));
    }

    /**
     * Extracts the message of the DBMS error that caused an oracle query to fail unexpectedly, from the
     * AssertionError that wraps it (see, e.g., ComparatorHelper#getResultSetFirstColumnAsString). Reproducers use it
     * to check that a reduced test case still triggers the same error, rather than an unrelated one introduced by the
     * reduction itself.
     *
     * @param error
     *            the AssertionError wrapping the DBMS error
     *
     * @return the message of the innermost cause that has one, or the error's own message
     */
    public static String getUnexpectedErrorMessage(AssertionError error) {
        String message = error.getMessage();
        Throwable current = error.getCause();
        while (current != null) {
            if (current.getMessage() != null) {
                message = current.getMessage();
            }
            current = current.getCause();
        }
        return message;
    }

    public static <E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> PredicateVariants<E, C> initializeTernaryPredicateVariants(
            PartitionGenerator<E, C> gen, E predicate) {
        if (gen == null) {
            throw new IllegalStateException();
        }
        if (predicate == null) {
            throw new IllegalStateException();
        }
        E negatedPredicate = gen.negatePredicate(predicate);
        if (negatedPredicate == null) {
            throw new IllegalStateException();
        }
        E isNullPredicate = gen.isNull(predicate);
        if (isNullPredicate == null) {
            throw new IllegalStateException();
        }
        return new PredicateVariants<>(predicate, negatedPredicate, isNullPredicate);
    }
}
