package sqlancer.presto.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoColumnReference;
import sqlancer.presto.ast.PrestoExpression;

public class PrestoQueryPartitioningGroupByTester extends PrestoQueryPartitioningBase {

    public PrestoQueryPartitioningGroupByTester(PrestoGlobalState state) {
        super(state);
        PrestoErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setGroupByExpressions(select.getFetchColumns());
        select.setWhereClause(null);
        String originalQueryString = PrestoToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

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

    @Override
    List<PrestoExpression> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream().map(c -> new PrestoColumnReference(c))
                .collect(Collectors.toList());
    }

}
