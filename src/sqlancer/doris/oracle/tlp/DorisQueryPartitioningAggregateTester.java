package sqlancer.doris.oracle.tlp;

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
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisCompositeDataType;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.ast.DorisAggregateOperation;
import sqlancer.doris.ast.DorisAggregateOperation.DorisAggregateFunction;
import sqlancer.doris.ast.DorisBinaryArithmeticOperation;
import sqlancer.doris.ast.DorisCastOperation;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.ast.DorisUnaryPostfixOperation.DorisUnaryPostfixOperator;
import sqlancer.doris.ast.DorisUnaryPrefixOperation.DorisUnaryPrefixOperator;
import sqlancer.doris.visitor.DorisExprToNode;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisQueryPartitioningAggregateTester extends DorisQueryPartitioningBase
        implements TestOracle<DorisGlobalState> {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public DorisQueryPartitioningAggregateTester(DorisGlobalState state) {
        super(state);
        DorisErrors.addExpressionErrors(errors);
        DorisErrors.addInsertErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        DorisAggregateFunction aggregateFunction = Randomly.fromOptions(DorisAggregateFunction.MAX,
                DorisAggregateFunction.MIN, DorisAggregateFunction.SUM, DorisAggregateFunction.COUNT,
                DorisAggregateFunction.AVG);
        NewFunctionNode<DorisExpression, DorisAggregateFunction> aggregate = (DorisAggregateOperation) gen
                .generateArgsForAggregate(aggregateFunction);
        List<Node<DorisExpression>> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add((DorisAggregateOperation) gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<Node<DorisExpression>> constants = new ArrayList<>();
            constants.add(
                    new DorisConstant.DorisIntConstant(Randomly.smallNumber() % select.getFetchColumns().size() + 1));
            select.setOrderByClauses(constants);
        }
        originalQuery = DorisToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
                "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
        if (firstResult == null && secondResult != null
                || firstResult != null && (!firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult))) {
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(DorisSelect select,
            NewFunctionNode<DorisExpression, DorisAggregateFunction> aggregate, List<Node<DorisExpression>> from) {
        String metamorphicQuery;
        Node<DorisExpression> whereClause = DorisExprToNode.cast(gen.generateExpression(DorisDataType.BOOLEAN));
        Node<DorisExpression> negatedClause = new NewUnaryPrefixOperatorNode<>(whereClause,
                DorisUnaryPrefixOperator.NOT);
        Node<DorisExpression> notNullClause = new NewUnaryPostfixOperatorNode<>(whereClause,
                DorisUnaryPostfixOperator.IS_NULL);
        List<Node<DorisExpression>> mappedAggregate = mapped(aggregate);
        DorisSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        DorisSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        DorisSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += DorisToStringVisitor.asString(leftSelect) + " UNION ALL "
                + DorisToStringVisitor.asString(middleSelect) + " UNION ALL "
                + DorisToStringVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
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
            if (!e.getMessage().contains("Not implemented type")) {
                throw new AssertionError(queryString, e);
            } else {
                throw new IgnoreMeException();
            }
        }
    }

    private List<Node<DorisExpression>> mapped(NewFunctionNode<DorisExpression, DorisAggregateFunction> aggregate) {

        DorisCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(Arrays.asList(aggregate));
        case AVG:
            NewFunctionNode<DorisExpression, DorisAggregateFunction> sum = new NewFunctionNode<>(aggregate.getArgs(),
                    DorisAggregateFunction.SUM);
            count = new DorisCastOperation(new NewFunctionNode<>(aggregate.getArgs(), DorisAggregateFunction.COUNT),
                    new DorisCompositeDataType(DorisDataType.FLOAT, 8));
            return aliasArgs(Arrays.asList(sum, count));
        case STDDEV_POP:
            NewFunctionNode<DorisExpression, DorisAggregateFunction> sumSquared = new NewFunctionNode<>(
                    Arrays.asList(new NewBinaryOperatorNode<>(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
                            DorisBinaryArithmeticOperation.DorisBinaryArithmeticOperator.MULTIPLICATION)),
                    DorisAggregateFunction.SUM);
            count = new DorisCastOperation(
                    new NewFunctionNode<DorisExpression, DorisAggregateFunction>(aggregate.getArgs(),
                            DorisAggregateFunction.COUNT),
                    new DorisCompositeDataType(DorisDataType.FLOAT, 8));
            NewFunctionNode<DorisExpression, DorisAggregateFunction> avg = new NewFunctionNode<>(aggregate.getArgs(),
                    DorisAggregateFunction.AVG);
            return aliasArgs(Arrays.asList(sumSquared, count, avg));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<Node<DorisExpression>> aliasArgs(List<Node<DorisExpression>> originalAggregateArgs) {
        List<Node<DorisExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<DorisExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<DorisExpression>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<DorisExpression, DorisAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
        case STDDEV_POP:
            return "sqrt(SUM(agg0)/SUM(agg1)-SUM(agg2)*SUM(agg2))";
        case AVG:
            return "SUM(agg0::FLOAT)/SUM(agg1)::FLOAT";
        case COUNT:
            return DorisAggregateFunction.SUM.toString() + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private DorisSelect getSelect(List<Node<DorisExpression>> aggregates, List<Node<DorisExpression>> from,
            Node<DorisExpression> whereClause, List<Node<DorisExpression>> joinList) {
        DorisSelect leftSelect = new DorisSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            //leftSelect.setGroupByExpressions(groupByExpression);    // todo: something maybe error in here
            DorisConstant groupByColumn = DorisConstant.createIntConstant(state.getRandomly().getInteger(1, targetTables.getColumns().size()));
            leftSelect.setGroupByExpressions(List.of(DorisExprToNode.cast(groupByColumn)));
        }
        return leftSelect;
    }

}
