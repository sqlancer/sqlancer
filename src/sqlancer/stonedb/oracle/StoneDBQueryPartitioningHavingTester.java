package sqlancer.stonedb.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBToStringVisitor;

public class StoneDBQueryPartitioningHavingTester extends StoneDBQueryPartitioningBase {
    public StoneDBQueryPartitioningHavingTester(StoneDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();
        // common to both original and combined query string
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        // specific to original query string
        select.setHavingClause(null);
        String originalQueryString = StoneDBToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        // specific to combined query string, will produce the same result as original query string in logic
        select.setHavingClause(predicate);
        String firstQueryString = StoneDBToStringVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = StoneDBToStringVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = StoneDBToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        // compare the result
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }
}
