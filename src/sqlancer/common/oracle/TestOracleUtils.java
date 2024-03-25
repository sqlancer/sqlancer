package sqlancer.common.oracle;

import java.sql.SQLException;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.gen.TLPGenerator;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public final class TestOracleUtils {

    public static class PredicateVariants<E extends Expression<C>, C extends AbstractTableColumn<?, ?>> {
        public E predicate;
        public E negatedPredicate;
        public E isNullPredicate;

        PredicateVariants(E predicate, E negatedPredicate, E isNullPredicate) {
            this.predicate = predicate;
            this.negatedPredicate = negatedPredicate;
            this.isNullPredicate = isNullPredicate;
        }
    }

    private TestOracleUtils() {
    }

    public static <J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> PredicateVariants<E, C> initializeTernaryPredicateVariants(
            TLPGenerator<J, E, T, C> gen) {
        if (gen == null) {
            throw new IllegalStateException();
        }
        E predicate = gen.generatePredicate();
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

    public static <T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> AbstractTables<T, C> getRandomTableNonEmptyTables(
            AbstractSchema<?, T> schema) {
        if (schema.getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        return new AbstractTables<>(Randomly.nonEmptySubset(schema.getDatabaseTables()));
    }

    public static <J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> TLPGenerator<J, E, T, C> initializeGenerator(
            GlobalState<?, S, ?> state, TLPGenerator<J, E, T, C> gen) throws SQLException {
        S s = state.getSchema();
        AbstractTables<T, C> targetTables = TestOracleUtils.getRandomTableNonEmptyTables(s);
        return gen.setTablesAndColumns(targetTables);
    }
}
