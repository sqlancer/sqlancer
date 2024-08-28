package sqlancer.databend.test.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendToStringVisitor;

public class DatabendQueryPartitioningWhereTester extends DatabendQueryPartitioningBase {

    public DatabendQueryPartitioningWhereTester(DatabendGlobalState state) {
        super(state);
        DatabendErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = DatabendToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        boolean orderBy = false;
        // if (orderBy) { //TODO 待开启
        // select.setOrderByClauses(gen.generateOrderBys());
        // }
        select.setWhereClause(predicate);
        String firstQueryString = DatabendToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = DatabendToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = DatabendToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }

}
