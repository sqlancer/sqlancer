package sqlancer.tidb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBTLPHavingOracle extends TiDBTLPBase implements TestOracle {

    public TiDBTLPHavingOracle(TiDBGlobalState state) {
        super(state);
        TiDBErrors.addExpressionHavingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = TiDBVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(predicate);
        String firstQueryString = TiDBVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = TiDBVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = TiDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected TiDBExpression generatePredicate() {
        return gen.generateHavingClause();
    }
}
