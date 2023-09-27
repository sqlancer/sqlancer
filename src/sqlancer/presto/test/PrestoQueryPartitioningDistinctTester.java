package sqlancer.presto.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoToStringVisitor;

public class PrestoQueryPartitioningDistinctTester extends PrestoQueryPartitioningBase {

    public PrestoQueryPartitioningDistinctTester(PrestoGlobalState state) {
        super(state);
        PrestoErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setDistinct(true);
        select.setWhereClause(null);
        String originalQueryString = PrestoToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        if (Randomly.getBoolean()) {
            select.setDistinct(false);
        }
        select.setWhereClause(predicate);
        String firstQueryString = PrestoToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = PrestoToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = PrestoToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, PrestoQueryPartitioningBase::canonicalizeResultValue);
    }

}
