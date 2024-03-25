package sqlancer.doris.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.TestOracle;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.visitor.DorisExprToNode;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisQueryPartitioningHavingTester extends DorisQueryPartitioningBase
        implements TestOracle<DorisGlobalState> {

    public DorisQueryPartitioningHavingTester(DorisGlobalState state) {
        super(state);
        DorisErrors.addExpressionErrors(errors);
        DorisErrors.addInsertErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBoolean()) {
            select.setWhereClause(DorisExprToNode.cast(gen.generateExpression(DorisSchema.DorisDataType.BOOLEAN)));
        }
        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            List<Node<DorisExpression>> constants = new ArrayList<>();
            constants.add(
                    new DorisConstant.DorisIntConstant(Randomly.smallNumber() % select.getFetchColumns().size() + 1));
            select.setOrderByClauses(constants);
        }
        select.setGroupByExpressions(groupByExpression);
        select.setHavingClause(null);
        String originalQueryString = DorisToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(DorisExprToNode.cast(predicate));
        String firstQueryString = DorisToStringVisitor.asString(select);
        select.setHavingClause(DorisExprToNode.cast(negatedPredicate));
        String secondQueryString = DorisToStringVisitor.asString(select);
        select.setHavingClause(DorisExprToNode.cast(isNullPredicate));
        String thirdQueryString = DorisToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }

    @Override
    protected DorisExpression generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<Node<DorisExpression>> generateFetchColumns() {
        gen.setAllowAggregateFunctions(true);
        List<Node<DorisExpression>> expressions = DorisExprToNode
                .casts(gen.generateExpressions(Randomly.smallNumber() + 1));
        gen.setAllowAggregateFunctions(false);
        return expressions;
    }

}
