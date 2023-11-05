package sqlancer.tidb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBTLPWhereOracle extends TiDBTLPBase {

    private String generatedQueryString;
    private Reproducer<TiDBGlobalState> reproducer;

    public TiDBTLPWhereOracle(TiDBGlobalState state) {
        super(state);
        TiDBErrors.addExpressionErrors(errors);
    }

    private class TiDBTLPWhereReproducer implements Reproducer<TiDBGlobalState> {
        final String firstQueryString;
        final String secondQueryString;
        final String thirdQueryString;
        final String originalQueryString;
        final List<String> resultSet;
        final boolean orderBy;

        TiDBTLPWhereReproducer(String firstQueryString, String secondQueryString, String thirdQueryString,
                String originalQueryString, List<String> resultSet, boolean orderBy) {
            this.firstQueryString = firstQueryString;
            this.secondQueryString = secondQueryString;
            this.thirdQueryString = thirdQueryString;
            this.originalQueryString = originalQueryString;
            this.resultSet = resultSet;
            this.orderBy = orderBy;
        }

        @Override
        public boolean bugStillTriggers(TiDBGlobalState globalState) {
            try {
                List<String> combinedString1 = new ArrayList<>();
                List<String> secondResultSet1 = ComparatorHelper.getCombinedResultSet(firstQueryString,
                        secondQueryString, thirdQueryString, combinedString1, !orderBy, globalState, errors);
                ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet1, originalQueryString,
                        combinedString1, globalState);
            } catch (AssertionError triggeredError) {
                return true;
            } catch (SQLException ignored) {
            }
            return false;
        }
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        super.check();
        select.setWhereClause(null);
        String originalQueryString = TiDBVisitor.asString(select);
        generatedQueryString = originalQueryString;
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setWhereClause(predicate);
        String firstQueryString = TiDBVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = TiDBVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = TiDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
        reproducer = new TiDBTLPWhereReproducer(firstQueryString, secondQueryString, thirdQueryString,
                originalQueryString, resultSet, orderBy);
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }

    @Override
    public Reproducer<TiDBGlobalState> getLastReproducer() {
        return reproducer;
    }

}
