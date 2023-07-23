package sqlancer.stonedb.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBToStringVisitor;

public class StoneDBQueryPartitioningWhereTester extends StoneDBQueryPartitioningBase {
    public StoneDBQueryPartitioningWhereTester(StoneDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();

        select.setWhereClause(null);
        String originalQueryString = StoneDBToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setWhereClause(predicate);
        String firstQueryString = StoneDBToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = StoneDBToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = StoneDBToStringVisitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        // compare the result
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }
}
