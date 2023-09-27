package sqlancer.materialize.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeVisitor;

public class MaterializeTLPWhereOracle extends MaterializeTLPBase {
    private String generatedQueryString;

    public MaterializeTLPWhereOracle(MaterializeGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        whereCheck();
    }

    protected void whereCheck() throws SQLException {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        String originalQueryString = MaterializeVisitor.asString(select);
        generatedQueryString = originalQueryString;
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setOrderByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = MaterializeVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = MaterializeVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = MaterializeVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
