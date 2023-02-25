package sqlancer.questdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.questdb.QuestDBErrors;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBToStringVisitor;

public class QuestDBQueryPartitioningWhereTester extends QuestDBQueryPartitioningBase {
    public QuestDBQueryPartitioningWhereTester(QuestDBGlobalState state) {
        super(state);
        QuestDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = QuestDBToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // Ignore OrderBy for now

        select.setWhereClause(predicate);
        String firstQueryString = QuestDBToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = QuestDBToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = QuestDBToStringVisitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, false, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, QuestDBQueryPartitioningBase::canonicalizeResultValue);
    }
}
