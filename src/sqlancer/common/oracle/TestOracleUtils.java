package sqlancer.common.oracle;

import java.io.IOException;
import java.sql.SQLException;

import org.postgresql.util.PSQLException;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.gen.PartitionGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
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

    public static void logQueryIfEnabled(SQLGlobalState<?, ?> globalState, String explainQuery) {
        // Log the query
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(explainQuery);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getAggregateResult(SQLGlobalState<?, ?> state, String queryString, ExpectedErrors errors,
            Boolean isEnabled) throws SQLException {

        // Log the query if enabled
        if (isEnabled) {
            logQueryIfEnabled(state, queryString);
        }

        // Execute the query
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getString(1);
            }
        } catch (SQLException e) {
            if (isEnabled || e instanceof PSQLException) {
                throw new AssertionError(queryString, e);
            } else if (!e.getMessage().contains("Not implemented type")) {
                throw new AssertionError(queryString, e);
            } else {
                throw new IgnoreMeException();
            }
        }

        return resultString;
    }

    public static void executeAndCompareQueries(SQLGlobalState<?, ?> state, String originalQuery,
            String metamorphicQuery, ExpectedErrors errors) throws SQLException {

        // Execute both queries
        String firstResult = getAggregateResult(state, originalQuery, errors, true);
        String secondResult = getAggregateResult(state, metamorphicQuery, errors, true);

        // Format and log the queries and their results
        String queryFormatString = "-- %s;\n-- result: %s";
        String firstQueryString = String.format(queryFormatString, originalQuery, firstResult);
        String secondQueryString = String.format(queryFormatString, metamorphicQuery, secondResult);
        state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));

        // Compare the results
        boolean resultsDiffer = firstResult == null && secondResult != null
                || firstResult != null && secondResult == null
                || firstResult != null && !firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult);

        if (resultsDiffer) {
            // Special handling for infinity values
            if (secondResult != null && secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }

            // Results don't match - throw assertion error
            String assertionMessage = String.format("the results mismatch!\n%s\n%s", firstQueryString,
                    secondQueryString);
            throw new AssertionError(assertionMessage);
        }
    }
}
