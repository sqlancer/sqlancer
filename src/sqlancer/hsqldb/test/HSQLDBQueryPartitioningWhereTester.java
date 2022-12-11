package sqlancer.hsqldb.test;

import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.hsqldb.HSQLDBProvider.HSQLDBGlobalState;
import sqlancer.hsqldb.HSQLDBToStringVisitor;

public class HSQLDBQueryPartitioningWhereTester extends HSQLDBQueryPartitioningBase {

    public HSQLDBQueryPartitioningWhereTester(HSQLDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();
        String originalQueryString = HSQLDBToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            select.setOrderByExpressions(expressionGenerator.generateOrderBys());
        }
        select.setWhereClause(predicate);
        String firstQueryString = HSQLDBToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = HSQLDBToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = HSQLDBToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
