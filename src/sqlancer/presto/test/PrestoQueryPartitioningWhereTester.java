package sqlancer.presto.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoToStringVisitor;

public class PrestoQueryPartitioningWhereTester extends PrestoQueryPartitioningBase {

    public PrestoQueryPartitioningWhereTester(PrestoGlobalState state) {
        super(state);
        PrestoErrors.addGroupByErrors(errors);
        PrestoErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = PrestoToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setWhereClause(predicate);
        String firstQueryString = PrestoToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = PrestoToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = PrestoToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, PrestoQueryPartitioningBase::canonicalizeResultValue);
    }

}
