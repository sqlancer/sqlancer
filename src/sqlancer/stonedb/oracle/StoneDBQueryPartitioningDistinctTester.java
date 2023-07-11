package sqlancer.stonedb.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBToStringVisitor;

public class StoneDBQueryPartitioningDistinctTester extends StoneDBQueryPartitioningBase {
    public StoneDBQueryPartitioningDistinctTester(StoneDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();

        select.setDistinct(true);
        select.setWhereClause(null);
        String originalQueryString = StoneDBToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        if (Randomly.getBoolean()) {
            select.setDistinct(false);
        }

        select.setWhereClause(predicate);
        String firstQueryString = StoneDBToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = StoneDBToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = StoneDBToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);

        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }
}
