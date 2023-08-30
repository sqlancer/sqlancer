package sqlancer.presto.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewAliasNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoAggregateFunction;
import sqlancer.presto.ast.PrestoCastFunction;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoSelect;
import sqlancer.presto.ast.PrestoUnaryPostfixOperation;
import sqlancer.presto.ast.PrestoUnaryPrefixOperation;

public class PrestoQueryPartitioningAggregateTester extends PrestoQueryPartitioningBase
        implements TestOracle<PrestoGlobalState> {

    private String firstResult;
    private String firstResultType;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public PrestoQueryPartitioningAggregateTester(PrestoGlobalState state) {
        super(state);
        PrestoErrors.addGroupByErrors(errors);
        PrestoErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        PrestoAggregateFunction aggregateFunction = Randomly.fromOptions(PrestoAggregateFunction.MAX,
                PrestoAggregateFunction.MIN, PrestoAggregateFunction.SUM, PrestoAggregateFunction.COUNT,
                PrestoAggregateFunction.AVG/* , PrestoAggregateFunction.STDDEV_POP */);
        List<Node<PrestoExpression>> aggregateArgs = gen.generateArgsForAggregate(aggregateFunction);
        NewFunctionNode<PrestoExpression, PrestoAggregateFunction> aggregate = new NewFunctionNode<>(aggregateArgs,
                aggregateFunction);
        select.setFetchColumns(List.of(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        originalQuery = PrestoToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        firstResultType = getAggregateResultType(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
                "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
        if (firstResultType.equals("VARBINARY") || firstResultType.equals("ARRAY(VARBINARY)")
                || firstResultType.equals("ARRAY(ARRAY(VARBINARY))")) {
            throw new IgnoreMeException();
        }
        if (firstResult == null && secondResult != null) {
            if (secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            throw new AssertionError();
        } else if (firstResult != null && !firstResult.contentEquals(secondResult)
                && !ComparatorHelper.isEqualDouble(firstResult, secondResult)) {
            if (secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(PrestoSelect select,
            NewFunctionNode<PrestoExpression, PrestoAggregateFunction> aggregate, List<Node<PrestoExpression>> from) {
        String metamorphicQuery;
        Node<PrestoExpression> whereClause = gen.generatePredicate();
        Node<PrestoExpression> negatedClause = new NewUnaryPrefixOperatorNode<>(whereClause,
                PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator.NOT);
        Node<PrestoExpression> notNullClause = new NewUnaryPostfixOperatorNode<>(whereClause,
                PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NULL);
        List<Node<PrestoExpression>> mappedAggregate = mapped(aggregate);
        PrestoSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        PrestoSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        PrestoSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += PrestoToStringVisitor.asString(leftSelect) + " UNION ALL "
                + PrestoToStringVisitor.asString(middleSelect) + " UNION ALL "
                + PrestoToStringVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) {
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors, false, false);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getString(1);
            }
            return resultString;
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }

            if (!e.getMessage().contains("Not implemented type")) {
                throw new AssertionError(queryString, e);
            } else {
                throw new IgnoreMeException();
            }
        }
    }

    private String getAggregateResultType(String queryString) {
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors, false, false);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getType(1);
            }
            return resultString;
        } catch (SQLException e) {
            if (!e.getMessage().contains("Not implemented type")) {
                throw new AssertionError(queryString, e);
            } else {
                throw new IgnoreMeException();
            }
        }
    }

    private List<Node<PrestoExpression>> mapped(NewFunctionNode<PrestoExpression, PrestoAggregateFunction> aggregate) {
        PrestoCastFunction count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(List.of(aggregate));
        case AVG:
            NewFunctionNode<PrestoExpression, PrestoAggregateFunction> sum = new NewFunctionNode<>(aggregate.getArgs(),
                    PrestoAggregateFunction.SUM);
            count = new PrestoCastFunction(new NewFunctionNode<>(aggregate.getArgs(), PrestoAggregateFunction.COUNT),
                    new PrestoCompositeDataType(PrestoDataType.FLOAT, 8, 0));
            return aliasArgs(Arrays.asList(sum, count));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<Node<PrestoExpression>> aliasArgs(List<Node<PrestoExpression>> originalAggregateArgs) {
        List<Node<PrestoExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<PrestoExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<PrestoExpression, PrestoAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
        case AVG:
            return "SUM(CAST(agg0 AS DOUBLE))/CAST(SUM(agg1) AS DOUBLE)";
        case COUNT:
            return PrestoAggregateFunction.SUM + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private PrestoSelect getSelect(List<Node<PrestoExpression>> aggregates, List<Node<PrestoExpression>> from,
            Node<PrestoExpression> whereClause, List<Node<PrestoExpression>> joinList) {
        PrestoSelect leftSelect = new PrestoSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
