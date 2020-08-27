package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractRowValue;

public abstract class PivotedQuerySynthesisBase<S extends GlobalState<?, ?>, R extends AbstractRowValue<?, ?, ?>, E>
        implements TestOracle {

    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final List<E> rectifiedPredicates = new ArrayList<>();
    protected final S globalState;
    protected R pivotRow;

    public PivotedQuerySynthesisBase(S globalState) {
        this.globalState = globalState;
    }

    @Override
    public final void check() throws SQLException {
        rectifiedPredicates.clear();
        Query pivotRowQuery = getQueryThatContainsAtLeastOneRow();
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(pivotRowQuery.getQueryString());
        }
        Query isContainedQuery = getContainedInQuery(pivotRowQuery);
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(isContainedQuery.getQueryString());
        }
        globalState.getState().getLocalState().log(isContainedQuery.getQueryString());
        boolean isContainedIn = isPivotRowContainedIn(isContainedQuery);
        if (!isContainedIn) {
            reportMissingPivotRow(pivotRowQuery);
        }
    }

    private boolean isPivotRowContainedIn(Query isContainedQuery) throws SQLException {
        try (SQLancerResultSet result = isContainedQuery.executeAndGet(globalState)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            return !result.isClosed();
        }
    }

    protected void reportMissingPivotRow(Query query) {
        globalState.getState().getLocalState().log("-- " + "pivot row values:\n");
        String expectedPivotRowString = pivotRow.asStringGroupedByTables();
        globalState.getState().getLocalState().log(expectedPivotRowString);

        StringBuilder sb = new StringBuilder("-- rectified predicates and their expected values:\n");
        for (E rectifiedPredicate : rectifiedPredicates) {
            sb.append("--");
            sb.append(asString(rectifiedPredicate).replace("\n", "\n-- "));
        }
        globalState.getState().getLocalState().log(sb.toString());
        throw new AssertionError(query);
    }

    protected abstract Query getContainedInQuery(Query pivotRowQuery) throws SQLException;

    protected abstract Query getQueryThatContainsAtLeastOneRow() throws SQLException;

    protected abstract String asString(E expr);

}
