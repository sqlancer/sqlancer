package sqlancer.cnosdb.oracle.tlp;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBComparatorHelper;
import sqlancer.cnosdb.CnosDBExpectedError;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CnosDBTLPWhereOracle extends CnosDBTLPBase {

    public CnosDBTLPWhereOracle(CnosDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();
        whereCheck();
    }

    protected void whereCheck() throws Exception {
        errors.addAll(CnosDBExpectedError.Errors());
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        String originalQueryString = CnosDBVisitor.asString(select);
        List<String> resultSet = CnosDBComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

        select.setOrderByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = CnosDBVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = CnosDBVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = CnosDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = CnosDBComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        CnosDBComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
