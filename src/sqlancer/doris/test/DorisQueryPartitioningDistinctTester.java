package sqlancer.doris.test;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisToStringVisitor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DorisQueryPartitioningDistinctTester extends DorisQueryPartitioningBase {

    public DorisQueryPartitioningDistinctTester(DorisGlobalState state) {
        super(state);
        DorisErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setDistinct(true);
        select.setWhereClause(null);
        String originalQueryString = DorisToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        if (Randomly.getBoolean()) {
            select.setDistinct(false);
        }
        select.setWhereClause(predicate);
        String firstQueryString = DorisToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = DorisToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = DorisToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, DorisQueryPartitioningBase::canonicalizeResultValue);
    }

}
