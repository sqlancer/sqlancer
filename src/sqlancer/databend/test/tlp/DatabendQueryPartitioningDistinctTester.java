package sqlancer.databend.test.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendToStringVisitor;

public class DatabendQueryPartitioningDistinctTester extends DatabendQueryPartitioningBase {

    public DatabendQueryPartitioningDistinctTester(DatabendGlobalState state) {
        super(state);
        DatabendErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setDistinct(true);
        // TODO 后期可以使用and来进行扩展
        // select.setWhereClause(DatabendExprToNode.cast(gen.generateExpression(DatabendSchema.DatabendDataType.BOOLEAN)));
        select.setWhereClause(null);
        String originalQueryString = DatabendToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        if (Randomly.getBoolean()) {
            select.setDistinct(false);
        }
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

}
