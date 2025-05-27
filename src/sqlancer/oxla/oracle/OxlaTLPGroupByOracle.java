package sqlancer.oxla.oracle;

import sqlancer.ComparatorHelper;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oxla.OxlaGlobalState;

import java.util.ArrayList;
import java.util.List;

public class OxlaTLPGroupByOracle extends OxlaTLPBase {
    private String originalQueryString;

    public OxlaTLPGroupByOracle(OxlaGlobalState state, ExpectedErrors errors) {
        super(state);
        this.errors.addAll(errors);
    }

    @Override
    public void check() throws Exception {
        super.check();
        select.setGroupByClause(select.getFetchColumns());
        select.setWhereClause(null);

        originalQueryString = select.asString();
        final List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        final String trueQuery = select.asString();

        select.setWhereClause(negatedPredicate);
        final String falseQuery = select.asString();

        select.setWhereClause(isNullPredicate);
        final String isNullQuery = select.asString();

        final List<String> combinedString = new ArrayList<>();
        final List<String> combinedResult = ComparatorHelper.getCombinedResultSetNoDuplicates(trueQuery, falseQuery, isNullQuery, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(originalResult, combinedResult, originalQueryString, combinedString, state);
    }

    @Override
    public String getLastQueryString() {
        return originalQueryString;
    }
}
