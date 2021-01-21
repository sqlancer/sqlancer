package sqlancer.common.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.SQLancerDBConnection;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractRowValue;

public abstract class PivotedQuerySynthesisBase<S extends GlobalState<?, ?, C>, R extends AbstractRowValue<?, ?, ?>, E, C extends SQLancerDBConnection>
        implements TestOracle {

    protected final ExpectedErrors errors = new ExpectedErrors();

    /**
     * The predicates used in WHERE and JOIN clauses, which yield TRUE for the pivot row.
     */
    protected final List<E> rectifiedPredicates = new ArrayList<>();

    /**
     * The generalization of a pivot row, as explained in the "Checking arbitrary expressions" paragraph of the PQS
     * paper.
     */
    protected List<E> pivotRowExpression = new ArrayList<>();
    protected final S globalState;
    protected R pivotRow;

    public PivotedQuerySynthesisBase(S globalState) {
        this.globalState = globalState;
    }

    @Override
    public final void check() throws Exception {
        rectifiedPredicates.clear();
        Query<C> pivotRowQuery = getRectifiedQuery();
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(pivotRowQuery.getQueryString());
        }
        Query<C> isContainedQuery = getContainmentCheckQuery(pivotRowQuery);
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(isContainedQuery.getQueryString());
        }
        globalState.getState().getLocalState().log(isContainedQuery.getQueryString());
        // combines step 6 and 7 described in the PQS paper
        boolean pivotRowIsContained = containsRows(isContainedQuery);
        if (!pivotRowIsContained) {
            reportMissingPivotRow(pivotRowQuery);
        }
    }

    /**
     * Checks whether the result set contains at least a single row.
     *
     * @param query
     *            the query for which to check whether its result set contains any rows
     *
     * @return true if at least one row is contained, false otherwise
     *
     * @throws Exception
     *             if the query unexpectedly fails
     */
    private boolean containsRows(Query<C> query) throws Exception {
        try (SQLancerResultSet result = query.executeAndGet(globalState)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            return !result.isClosed();
        }
    }

    protected void reportMissingPivotRow(Query<?> query) {
        globalState.getState().getLocalState().log("-- pivot row values:");
        String expectedPivotRowString = pivotRow.asStringGroupedByTables();
        globalState.getState().getLocalState().log(expectedPivotRowString);

        StringBuilder sb = new StringBuilder();
        if (!rectifiedPredicates.isEmpty()) {
            sb.append("--\n-- rectified predicates and their expected values:\n");
            for (E rectifiedPredicate : rectifiedPredicates) {
                sb.append("--");
                sb.append(getExpectedValues(rectifiedPredicate).replace("\n", "\n-- "));
            }
            sb.append("\n");
        }
        if (!pivotRowExpression.isEmpty()) {
            sb.append("-- pivot row expressions and their expected values:\n");
            for (E pivotRowExpression : pivotRowExpression) {
                sb.append("--");
                sb.append(getExpectedValues(pivotRowExpression).replace("\n", "\n--"));
                sb.append("\n");
            }
        }
        globalState.getState().getLocalState().log(sb.toString());
        throw new AssertionError(query);
    }

    /**
     * Gets a query that checks whether the pivot row is contained in the result. If the pivot row is contained, the
     * query will fetch at least one row. If the pivot row is not contained, no rows will be fetched. This corresponds
     * to step 7 described in the PQS paper.
     *
     * @param pivotRowQuery
     *            the query that is guaranteed to fetch the pivot row, potentially among other rows
     *
     * @return a query that checks whether the pivot row is contained in pivotRowQuery
     *
     * @throws Exception
     *             if an unexpected error occurs
     */
    protected abstract Query<C> getContainmentCheckQuery(Query<?> pivotRowQuery) throws Exception;

    /**
     * Obtains a rectified query (i.e., a query that is guaranteed to fetch the pivot row. This corresponds to steps 2-5
     * of the PQS paper.
     *
     * @return the rectified query
     *
     * @throws Exception
     *             if an unexpected error occurs
     */
    protected abstract Query<C> getRectifiedQuery() throws Exception;

    /**
     * Prints the value to which the expression is expected to evaluate, and then recursively prints the subexpressions'
     * expected values.
     *
     * @param expr
     *            the expression whose expected value should be printed
     *
     * @return a string representing the expected value of the expression and its subexpressions
     */
    protected abstract String getExpectedValues(E expr);

}
