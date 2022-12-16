package sqlancer.databend.test.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewAliasNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendAggregateOperation;
import sqlancer.databend.ast.DatabendAggregateOperation.DatabendAggregateFunction;
import sqlancer.databend.ast.DatabendBinaryArithmeticOperation.DatabendBinaryArithmeticOperator;
import sqlancer.databend.ast.DatabendCastOperation;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.ast.DatabendUnaryPostfixOperation.DatabendUnaryPostfixOperator;
import sqlancer.databend.ast.DatabendUnaryPrefixOperation.DatabendUnaryPrefixOperator;

public class DatabendQueryPartitioningAggregateTester extends DatabendQueryPartitioningBase {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public DatabendQueryPartitioningAggregateTester(DatabendGlobalState state) {
        super(state);
        DatabendErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        DatabendAggregateFunction aggregateFunction = Randomly.fromOptions(DatabendAggregateFunction.MAX,
                DatabendAggregateFunction.MIN, DatabendAggregateFunction.SUM, DatabendAggregateFunction.COUNT,
                DatabendAggregateFunction.AVG/* , DatabendAggregateFunction.STDDEV_POP */);
        NewFunctionNode<DatabendExpression, DatabendAggregateFunction> aggregate = (DatabendAggregateOperation) gen
                .generateArgsForAggregate(aggregateFunction);
        List<Node<DatabendExpression>> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add((DatabendAggregateOperation) gen.generateAggregate()); // TODO 更换成非聚合函数
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        // if (Randomly.getBooleanWithRatherLowProbability()) {
        // select.setOrderByExpressions(gen.generateOrderBys());
        // }
        originalQuery = DatabendToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
                "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
        if (firstResult == null && secondResult != null
                || firstResult != null && (!firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult))) {
            if (secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(DatabendSelect select,
            NewFunctionNode<DatabendExpression, DatabendAggregateFunction> aggregate,
            List<Node<DatabendExpression>> from) {
        String metamorphicQuery;
        Node<DatabendExpression> whereClause = DatabendExprToNode
                .cast(gen.generateExpression(DatabendDataType.BOOLEAN));
        Node<DatabendExpression> negatedClause = new NewUnaryPrefixOperatorNode<>(whereClause,
                DatabendUnaryPrefixOperator.NOT);
        Node<DatabendExpression> notNullClause = new NewUnaryPostfixOperatorNode<>(whereClause,
                DatabendUnaryPostfixOperator.IS_NULL);
        List<Node<DatabendExpression>> mappedAggregate = mapped(aggregate);
        DatabendSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        DatabendSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        DatabendSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += DatabendToStringVisitor.asString(leftSelect) + " UNION ALL "
                + DatabendToStringVisitor.asString(middleSelect) + " UNION ALL "
                + DatabendToStringVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        String resultString = null;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                try {
                    resultString = result.getString(1);
                } catch (Exception e) {
                    throw new IgnoreMeException(); // TODO 超过integer范围无法格式化异常，还未有解决方案
                }
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

    private List<Node<DatabendExpression>> mapped(
            NewFunctionNode<DatabendExpression, DatabendAggregateFunction> aggregate) {
        DatabendCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(Arrays.asList(aggregate));
        case AVG:
            NewFunctionNode<DatabendExpression, DatabendAggregateFunction> sum = new NewFunctionNode<>(
                    aggregate.getArgs(), DatabendAggregateFunction.SUM);
            count = new DatabendCastOperation(
                    new NewFunctionNode<>(aggregate.getArgs(), DatabendAggregateFunction.COUNT),
                    new DatabendCompositeDataType(DatabendDataType.FLOAT, 8));
            return aliasArgs(Arrays.asList(sum, count));
        case STDDEV_POP:
            NewFunctionNode<DatabendExpression, DatabendAggregateFunction> sumSquared = new NewFunctionNode<>(
                    Arrays.asList(new NewBinaryOperatorNode<>(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
                            DatabendBinaryArithmeticOperator.MULTIPLICATION)),
                    DatabendAggregateFunction.SUM);
            count = new DatabendCastOperation(
                    new NewFunctionNode<DatabendExpression, DatabendAggregateFunction>(aggregate.getArgs(),
                            DatabendAggregateFunction.COUNT),
                    new DatabendCompositeDataType(DatabendDataType.FLOAT, 8));
            NewFunctionNode<DatabendExpression, DatabendAggregateFunction> avg = new NewFunctionNode<>(
                    aggregate.getArgs(), DatabendAggregateFunction.AVG);
            return aliasArgs(Arrays.asList(sumSquared, count, avg));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<Node<DatabendExpression>> aliasArgs(List<Node<DatabendExpression>> originalAggregateArgs) {
        List<Node<DatabendExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<DatabendExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<DatabendExpression>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<DatabendExpression, DatabendAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
        case STDDEV_POP:
            return "sqrt(SUM(agg0)/SUM(agg1)-SUM(agg2)*SUM(agg2))";
        case AVG:
            return "SUM(agg0)/SUM(agg1)";
        case COUNT:
            return DatabendAggregateFunction.SUM.toString() + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private DatabendSelect getSelect(List<Node<DatabendExpression>> aggregates, List<Node<DatabendExpression>> from,
            Node<DatabendExpression> whereClause, List<Node<DatabendExpression>> joinList) {
        DatabendSelect select = new DatabendSelect();
        select.setFetchColumns(aggregates);
        select.setFromList(from);
        select.setWhereClause(whereClause);
        select.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setGroupByExpressions(List.of(DatabendExprToNode.cast(gen.generateConstant(DatabendDataType.INT)))); // TODO
                                                                                                                        // 仍可加强
        }
        return select;
    }

}
