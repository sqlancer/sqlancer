package sqlancer.hive.oracle;

import java.util.List;
import java.util.ArrayList;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.hive.HiveErrors;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveToStringVisitor;

public class HiveTLPWhereOracle extends HiveTLPBase {

    public HiveTLPWhereOracle(HiveGlobalState state) {
        super(state);
        HiveErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        super.check();
        String originalQueryString = HiveToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state); 

        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            select.setOrderByClauses(gen.generateOrderBys());
        }

        select.setWhereClause(predicate);
        String firstQueryString = HiveToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = HiveToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = HiveToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }
}
