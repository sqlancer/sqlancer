package sqlancer.stonedb.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBToStringVisitor;
import sqlancer.stonedb.ast.StoneDBExpression;

public class StoneDBQueryPartitioningGroupByTester extends StoneDBQueryPartitioningBase {
    public StoneDBQueryPartitioningGroupByTester(StoneDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();
        // common to both original and combined query string
        select.setGroupByExpressions(select.getFetchColumns());
        // specific to original query string
        select.setWhereClause(null);
        String originalQueryString = StoneDBToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        // specific to combined query string, will produce the same result as original query string in logic
        select.setWhereClause(predicate);
        String firstQueryString = StoneDBToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = StoneDBToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = StoneDBToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        // compare the result
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }

    @Override
    List<Node<StoneDBExpression>> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ColumnReferenceNode<StoneDBExpression, StoneDBColumn>(c)).collect(Collectors.toList());
    }
}
