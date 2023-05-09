package sqlancer.doris.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.visitor.DorisExprToNode;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisQueryPartitioningDistinctTester extends DorisQueryPartitioningBase {

    public DorisQueryPartitioningDistinctTester(DorisGlobalState state) {
        super(state);
        DorisErrors.addExpressionErrors(errors);
        DorisErrors.addInsertErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setDistinct(DorisSelect.DorisSelectDistinctType.getRandomWithoutNull());
        select.setWhereClause(null);
        String originalQueryString = DorisToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        if (Randomly.getBoolean()) {
            select.setDistinct(false);
        }
        select.setWhereClause(DorisExprToNode.cast(predicate));
        String firstQueryString = DorisToStringVisitor.asString(select);
        select.setWhereClause(DorisExprToNode.cast(negatedPredicate));
        String secondQueryString = DorisToStringVisitor.asString(select);
        select.setWhereClause(DorisExprToNode.cast(isNullPredicate));
        String thirdQueryString = DorisToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, DorisQueryPartitioningBase::canonicalizeResultValue);
    }

}
