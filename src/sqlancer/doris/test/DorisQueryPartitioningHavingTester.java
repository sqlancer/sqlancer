package sqlancer.doris.test;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.TestOracle;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisToStringVisitor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DorisQueryPartitioningHavingTester extends DorisQueryPartitioningBase
        implements TestOracle<DorisGlobalState> {

    public DorisQueryPartitioningHavingTester(DorisGlobalState state) {
        super(state);
        DorisErrors.addGroupByErrors(errors);
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
        String originalQueryString = DorisToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(predicate);
        String firstQueryString = DorisToStringVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = DorisToStringVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = DorisToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, DorisQueryPartitioningBase::canonicalizeResultValue);
    }

    @Override
    protected Node<DorisExpression> generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<Node<DorisExpression>> generateFetchColumns() {
        return Arrays.asList(gen.generateHavingClause());
    }

}
