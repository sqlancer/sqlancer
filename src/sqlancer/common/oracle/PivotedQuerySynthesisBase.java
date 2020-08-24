package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.GlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
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
        boolean isContainedIn = isContainedIn(pivotRowQuery);
        if (!isContainedIn) {
            reportMissingPivotRow(pivotRowQuery);
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

    protected abstract boolean isContainedIn(Query pivotRowQuery) throws SQLException;

    protected abstract Query getQueryThatContainsAtLeastOneRow() throws SQLException;

    protected abstract String asString(E expr);

}
