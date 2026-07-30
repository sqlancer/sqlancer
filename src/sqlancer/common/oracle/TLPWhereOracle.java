package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public class TLPWhereOracle<Z extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;

    private TLPWhereGenerator<Z, J, E, T, C> gen;
    private final ExpectedErrors errors;

    private Reproducer<G> reproducer;
    private String generatedQueryString;

    // A side's result set, together with the human-readable combined-query strings that
    // getCombinedResultSet fills in for the transformed side (unused, and null, for the original side)
    private static final class TLPResultSet {
        final List<String> resultSet;
        final List<String> combinedString;

        TLPResultSet(List<String> resultSet, List<String> combinedString) {
            this.resultSet = resultSet;
            this.combinedString = combinedString;
        }
    }

    private class TLPWhereReproducer extends AbstractComparisonReproducer<G, TLPResultSet> {
        final String firstQueryString;
        final String secondQueryString;
        final String thirdQueryString;
        final String originalQueryString;
        final boolean orderBy;

        TLPWhereReproducer(String firstQueryString, String secondQueryString, String thirdQueryString,
                String originalQueryString, boolean orderBy, String expectedErrorMessage) {
            super(expectedErrorMessage);
            this.firstQueryString = firstQueryString;
            this.secondQueryString = secondQueryString;
            this.thirdQueryString = thirdQueryString;
            this.originalQueryString = originalQueryString;
            this.orderBy = orderBy;
        }

        @Override
        protected boolean hasTransformedSide() {
            return firstQueryString != null;
        }

        @Override
        protected TLPResultSet evaluateOriginal(G globalState) throws SQLException {
            return new TLPResultSet(
                    ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, globalState), null);
        }

        @Override
        protected TLPResultSet evaluateTransformed(G globalState) throws SQLException {
            List<String> combinedString = new ArrayList<>();
            List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                    thirdQueryString, combinedString, !orderBy, globalState, errors);
            return new TLPResultSet(secondResultSet, combinedString);
        }

        @Override
        protected boolean sidesDiffer(TLPResultSet original, TLPResultSet transformed, G globalState) {
            try {
                ComparatorHelper.assumeResultSetsAreEqual(original.resultSet, transformed.resultSet,
                        originalQueryString, transformed.combinedString, globalState);
            } catch (AssertionError resultSetMismatch) {
                return true;
            }
            return false;
        }

        @Override
        protected String mismatchHeaderLine() {
            return "-- On the database set up by the statements above, the result sets of the following"
                    + " queries mismatch:";
        }

        @Override
        protected void appendQueryLines(StringBuilder sb) {
            sb.append("-- ").append(originalQueryString).append(';').append(System.lineSeparator());
            if (firstQueryString != null) {
                if (orderBy) {
                    sb.append("-- ").append(firstQueryString).append(';').append(System.lineSeparator());
                    sb.append("-- ").append(secondQueryString).append(';').append(System.lineSeparator());
                    sb.append("-- ").append(thirdQueryString).append(';').append(System.lineSeparator());
                } else {
                    sb.append("-- ").append(firstQueryString).append(" UNION ALL ").append(secondQueryString)
                            .append(" UNION ALL ").append(thirdQueryString).append(';').append(System.lineSeparator());
                }
            }
        }
    }

    public TLPWhereOracle(G state, TLPWhereGenerator<Z, J, E, T, C> gen, ExpectedErrors expectedErrors) {
        if (state == null || gen == null || expectedErrors == null) {
            throw new IllegalArgumentException("Null variables used to initialize test oracle.");
        }
        this.state = state;
        this.gen = gen;
        this.errors = expectedErrors;
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        S s = state.getSchema();
        AbstractTables<T, C> targetTables = TestOracleUtils.getRandomTableNonEmptyTables(s);
        gen = gen.setTablesAndColumns(targetTables);

        Select<J, E, T, C> select = gen.generateSelect();

        boolean shouldCreateDummy = true;
        select.setFetchColumns(gen.generateFetchColumns(shouldCreateDummy));
        select.setJoinClauses(gen.getRandomJoinClauses());
        select.setFromList(gen.getTableRefs());
        select.setWhereClause(null);

        String originalQueryString = select.asString();
        generatedQueryString = originalQueryString;
        List<String> firstResultSet;
        try {
            firstResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        } catch (AssertionError unexpectedError) {
            reproducer = new TLPWhereReproducer(null, null, null, originalQueryString, false,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            throw unexpectedError;
        }

        boolean orderBy = Randomly.getBooleanWithSmallProbability();
        if (orderBy) {
            select.setOrderByClauses(gen.generateOrderBys());
        }

        TestOracleUtils.PredicateVariants<E, C> predicates = TestOracleUtils.initializeTernaryPredicateVariants(gen,
                gen.generateBooleanExpression());
        select.setWhereClause(predicates.predicate);
        String firstQueryString = select.asString();
        select.setWhereClause(predicates.negatedPredicate);
        String secondQueryString = select.asString();
        select.setWhereClause(predicates.isNullPredicate);
        String thirdQueryString = select.asString();

        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet;
        try {
            secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                    thirdQueryString, combinedString, !orderBy, state, errors);
        } catch (AssertionError unexpectedError) {
            reproducer = new TLPWhereReproducer(firstQueryString, secondQueryString, thirdQueryString,
                    originalQueryString, orderBy, TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            throw unexpectedError;
        }

        reproducer = new TLPWhereReproducer(firstQueryString, secondQueryString, thirdQueryString, originalQueryString,
                orderBy, null);
        ComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    public Reproducer<G> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
