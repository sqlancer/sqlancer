package sqlancer.doris.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.ast.DorisColumnReference;
import sqlancer.doris.ast.DorisExpression;
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
        select.setGroupByExpressions(select.getFetchColumns());
        select.setWhereClause(null);
        String originalQueryString = DorisToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        List<String> queryStrings = getQueryStrings(select);
        List<String> combinedString = new ArrayList<>();

        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(queryStrings.get(0),
                queryStrings.get(1), queryStrings.get(2), combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }

    @Override
    List<DorisExpression> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream().map(c -> new DorisColumnReference(c))
                .collect(Collectors.toList());
    }

}
