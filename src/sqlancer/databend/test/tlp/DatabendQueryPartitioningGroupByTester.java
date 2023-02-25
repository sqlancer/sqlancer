package sqlancer.databend.test.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendExpression;

public class DatabendQueryPartitioningGroupByTester extends DatabendQueryPartitioningBase {

    public DatabendQueryPartitioningGroupByTester(DatabendGlobalState state) {
        super(state);
        DatabendErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setGroupByExpressions(groupByExpression);
        select.setWhereClause(null);
        String originalQueryString = DatabendToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(DatabendExprToNode.cast(predicate));
        String firstQueryString = DatabendToStringVisitor.asString(select);
        select.setWhereClause(DatabendExprToNode.cast(negatedPredicate));
        String secondQueryString = DatabendToStringVisitor.asString(select);
        select.setWhereClause(DatabendExprToNode.cast(isNullPredicate));
        String thirdQueryString = DatabendToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, DatabendQueryPartitioningBase::canonicalizeResultValue);
    }

    @Override
    List<Node<DatabendExpression>> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ColumnReferenceNode<DatabendExpression, DatabendColumn>(c)).collect(Collectors.toList());
    }

}
