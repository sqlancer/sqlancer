package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.function.Function;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public class NoRECOracle<Z extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;

    private NoRECGenerator<Z, J, E, T, C> gen;
    private final ExpectedErrors errors;

    private Reproducer<G> reproducer;
    private String lastQueryString;

    private static class NoRECReproducer<G extends SQLGlobalState<?, ?>>
            extends AbstractComparisonReproducer<G, Integer> {
        private final Function<G, Integer> optimizedQuery;
        private final Function<G, Integer> unoptimizedQuery;
        private final String optimizedQueryString;
        private final String unoptimizedQueryString;

        NoRECReproducer(Function<G, Integer> optimizedQuery, Function<G, Integer> unoptimizedQuery,
                String optimizedQueryString, String unoptimizedQueryString) {
            this.optimizedQuery = optimizedQuery;
            this.unoptimizedQuery = unoptimizedQuery;
            this.optimizedQueryString = optimizedQueryString;
            this.unoptimizedQueryString = unoptimizedQueryString;
        }

        @Override
        protected Integer evaluateOriginal(G globalState) {
            return optimizedQuery.apply(globalState);
        }

        @Override
        protected Integer evaluateTransformed(G globalState) {
            return unoptimizedQuery.apply(globalState);
        }

        @Override
        protected boolean sidesDiffer(Integer optimizedCount, Integer unoptimizedCount, G globalState) {
            if (optimizedCount == -1 || unoptimizedCount == -1) {
                return false;
            }
            return optimizedCount.intValue() != unoptimizedCount.intValue();
        }

        @Override
        protected String mismatchHeaderLine() {
            return "-- On the database set up by the statements above, the row counts of the following"
                    + " queries mismatch:";
        }

        @Override
        protected void appendQueryLines(StringBuilder sb) {
            renderQueryLines(sb, optimizedQueryString, unoptimizedQueryString);
        }
    }

    // Renders the failing queries as commented lines, shared by the mismatch and the unexpected-error reproducers.
    private static void renderQueryLines(StringBuilder sb, String optimizedQueryString, String unoptimizedQueryString) {
        sb.append("-- optimized: ").append(optimizedQueryString).append(';').append(System.lineSeparator());
        sb.append("-- unoptimized: ").append(unoptimizedQueryString).append(';').append(System.lineSeparator());
    }

    // Builds the reproducer for an unexpected DBMS error, which re-runs both queries and checks the same error fires.
    private static <G extends SQLGlobalState<?, ?>> UnexpectedErrorReproducer<G> errorReproducer(
            Function<G, Integer> optimizedQuery, Function<G, Integer> unoptimizedQuery, String optimizedQueryString,
            String unoptimizedQueryString, String expectedErrorMessage) {
        UnexpectedErrorReproducer.Execution<G> execution = globalState -> {
            optimizedQuery.apply(globalState);
            unoptimizedQuery.apply(globalState);
        };
        StringBuilder sb = new StringBuilder();
        renderQueryLines(sb, optimizedQueryString, unoptimizedQueryString);
        return new UnexpectedErrorReproducer<>(execution, expectedErrorMessage, sb.toString());
    }

    public NoRECOracle(G state, NoRECGenerator<Z, J, E, T, C> gen, ExpectedErrors expectedErrors) {
        if (state == null || gen == null || expectedErrors == null) {
            throw new IllegalArgumentException("Null variables used to initialize test oracle.");
        }
        this.state = state;
        this.gen = gen;
        this.errors = expectedErrors;
        this.reproducer = null;
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        S schema = state.getSchema();
        AbstractTables<T, C> targetTables = TestOracleUtils.getRandomTableNonEmptyTables(schema);
        gen = gen.setTablesAndColumns(targetTables);

        Z select = gen.generateSelect();
        select.setJoinClauses(gen.getRandomJoinClauses());
        select.setFromList(gen.getTableRefs());

        E randomWhereCondition = gen.generateBooleanExpression();

        boolean shouldUseAggregate = Randomly.getBoolean();
        String optimizedQueryString = gen.generateOptimizedQueryString(select, randomWhereCondition,
                shouldUseAggregate);
        lastQueryString = optimizedQueryString;
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(optimizedQueryString);
        }

        String unoptimizedQueryString = gen.generateUnoptimizedQueryString(select, randomWhereCondition);
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(unoptimizedQueryString);
        }

        Function<G, Integer> optimizedQuery = state -> shouldUseAggregate
                ? extractCounts(optimizedQueryString, errors, state) : countRows(optimizedQueryString, errors, state);
        Function<G, Integer> unoptimizedQuery = state -> extractCounts(unoptimizedQueryString, errors, state);

        int optimizedCount;
        int unoptimizedCount;
        try {
            optimizedCount = optimizedQuery.apply(state);
            unoptimizedCount = unoptimizedQuery.apply(state);
        } catch (AssertionError unexpectedError) {
            reproducer = errorReproducer(optimizedQuery, unoptimizedQuery, optimizedQueryString, unoptimizedQueryString,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            throw unexpectedError;
        }

        if (optimizedCount == -1 || unoptimizedCount == -1) {
            throw new IgnoreMeException();
        }

        if (unoptimizedCount != optimizedCount) {
            reproducer = new NoRECReproducer<>(optimizedQuery, unoptimizedQuery, optimizedQueryString,
                    unoptimizedQueryString);

            String queryFormatString = "-- %s;\n-- count: %d";
            String firstQueryStringWithCount = String.format(queryFormatString, optimizedQueryString, optimizedCount);
            String secondQueryStringWithCount = String.format(queryFormatString, unoptimizedQueryString,
                    unoptimizedCount);
            state.getState().getLocalState()
                    .log(String.format("%s\n%s", firstQueryStringWithCount, secondQueryStringWithCount));
            String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", optimizedCount,
                    unoptimizedCount, firstQueryStringWithCount, secondQueryStringWithCount);
            throw new AssertionError(assertionMessage);
        }
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }

    @Override
    public Reproducer<G> getLastReproducer() {
        return reproducer;
    }

    private int countRows(String queryString, ExpectedErrors errors, SQLGlobalState<?, ?> state) {
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors, false, false);

        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null) {
                return -1;
            } else {
                try {
                    while (rs.next()) {
                        count++;
                    }
                } catch (SQLException e) {
                    count = -1;
                }
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(q.getQueryString(), e);
        }
        return count;
    }

    private int extractCounts(String queryString, ExpectedErrors errors, SQLGlobalState<?, ?> state) {
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors, false, false);
        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null) {
                return -1;
            } else {
                try {
                    while (rs.next()) {
                        count += rs.getInt(1);
                    }
                } catch (SQLException e) {
                    count = -1;
                }
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(q.getQueryString(), e);
        }
        return count;
    }

}
