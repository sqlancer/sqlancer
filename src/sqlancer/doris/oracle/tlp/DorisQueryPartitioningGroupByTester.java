package sqlancer.doris.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.visitor.DorisExprToNode;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisQueryPartitioningGroupByTester extends DorisQueryPartitioningBase {

    public DorisQueryPartitioningGroupByTester(DorisGlobalState state) {
        super(state);
        DorisErrors.addExpressionErrors(errors);
        DorisErrors.addInsertErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setGroupByExpressions(groupByExpression);
        select.setWhereClause(null);
        String originalQueryString = DorisToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(DorisExprToNode.cast(predicate));
        String firstQueryString = DorisToStringVisitor.asString(select);
        select.setWhereClause(DorisExprToNode.cast(negatedPredicate));
        String secondQueryString = DorisToStringVisitor.asString(select);
        select.setWhereClause(DorisExprToNode.cast(isNullPredicate));
        String thirdQueryString = DorisToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }

    @Override
    List<Node<DorisExpression>> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ColumnReferenceNode<DorisExpression, DorisColumn>(c)).collect(Collectors.toList());
    }

}
