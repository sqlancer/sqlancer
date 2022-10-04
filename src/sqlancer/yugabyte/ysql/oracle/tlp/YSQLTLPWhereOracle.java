package sqlancer.yugabyte.ysql.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLVisitor;

public class YSQLTLPWhereOracle extends YSQLTLPBase {

    public YSQLTLPWhereOracle(YSQLGlobalState state) {
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
        String originalQueryString = YSQLVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setOrderByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = YSQLVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = YSQLVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = YSQLVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
