package sqlancer.cnosdb.oracle.tlp;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBComparatorHelper;
import sqlancer.cnosdb.CnosDBExpectedError;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBVisitor;
import sqlancer.cnosdb.ast.CnosDBExpression;

public class CnosDBTLPHavingOracle extends CnosDBTLPBase {

    public CnosDBTLPHavingOracle(CnosDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();
        havingCheck();
    }

    protected void havingCheck() throws Exception {
        errors.addAll(CnosDBExpectedError.expectedErrors());
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(CnosDBDataType.BOOLEAN));
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = CnosDBVisitor.asString(select);
        List<String> resultSet = CnosDBComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        select.setHavingClause(predicate);
        String firstQueryString = CnosDBVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = CnosDBVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = CnosDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = CnosDBComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        CnosDBComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected CnosDBExpression generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<CnosDBExpression> generateFetchColumns() {
        List<CnosDBExpression> expressions = gen.allowAggregates(true).generateExpressions(Randomly.smallNumber() + 1);
        gen.allowAggregates(false);
        return expressions;
    }

}
