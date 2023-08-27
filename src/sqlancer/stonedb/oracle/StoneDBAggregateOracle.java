package sqlancer.stonedb.oracle;

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
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.stonedb.StoneDBErrors;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBCompositeDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.StoneDBToStringVisitor;
import sqlancer.stonedb.ast.StoneDBAggregate.StoneDBAggregateFunction;
import sqlancer.stonedb.ast.StoneDBExpression;
import sqlancer.stonedb.ast.StoneDBSelect;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator.StoneDBCastOperation;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator.StoneDBUnaryPostfixOperator;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator.StoneDBUnaryPrefixOperator;

public class StoneDBAggregateOracle extends StoneDBQueryPartitioningBase {

    public StoneDBAggregateOracle(StoneDBGlobalState state) {
        super(state);
        StoneDBErrors.addExpectedExpressionErrors(state, errors);
    }

    @Override
    public void check() throws Exception {
        super.check();

        StoneDBAggregateFunction aggregateFunction = Randomly.fromOptions(StoneDBAggregateFunction.values());
        NewFunctionNode<StoneDBExpression, StoneDBAggregateFunction> aggregate = gen
                .generateAggregateAndArgs(aggregateFunction);

        List<Node<StoneDBExpression>> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        select.setFetchColumns(fetchColumns);

        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        String originalQuery = StoneDBToStringVisitor.asString(select);
        String originalResult = getAggregateResult(originalQuery);

        String metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        String metamorphicResult = getAggregateResult(metamorphicQuery);

        String line1 = "--" + originalQuery + ";";
        String line2 = "--" + originalResult + ";";
        String line3 = "--" + originalResult + ";";
        String line4 = "--" + metamorphicResult + ";";
        String output = String.join(System.lineSeparator(), line1, line2, line3, line4);
        state.getState().getLocalState().log(output);

        if (originalResult == null && metamorphicResult != null
                || originalResult != null && (!originalResult.contentEquals(metamorphicResult)
                        && !ComparatorHelper.isEqualDouble(originalResult, metamorphicResult))) {
            throw new AssertionError("aggregate result mismatch!" + System.lineSeparator() + output);
        }
    }

    private String createMetamorphicUnionQuery(StoneDBSelect select,
            NewFunctionNode<StoneDBExpression, StoneDBAggregateFunction> aggregate,
            List<Node<StoneDBExpression>> from) {
        String metamorphicQuery;
        Node<StoneDBExpression> whereClause = gen.generateExpression();

        Node<StoneDBExpression> negatedClause = new NewUnaryPrefixOperatorNode<>(whereClause,
                StoneDBUnaryPrefixOperator.NOT);
        Node<StoneDBExpression> notNullClause = new NewUnaryPostfixOperatorNode<>(whereClause,
                StoneDBUnaryPostfixOperator.IS_NULL);
        List<Node<StoneDBExpression>> mappedAggregate = mapped(aggregate);
        StoneDBSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        StoneDBSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        StoneDBSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += StoneDBToStringVisitor.asString(leftSelect) + " UNION ALL "
                + StoneDBToStringVisitor.asString(middleSelect) + " UNION ALL "
                + StoneDBToStringVisitor.asString(rightSelect);
        metamorphicQuery += ") as result";
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

    private List<Node<StoneDBExpression>> mapped(
            NewFunctionNode<StoneDBExpression, StoneDBAggregateFunction> aggregate) {

        StoneDBCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(List.of(aggregate));
        case AVG:
            NewFunctionNode<StoneDBExpression, StoneDBAggregateFunction> sum = new NewFunctionNode<>(
                    aggregate.getArgs(), StoneDBAggregateFunction.SUM);
            count = new StoneDBCastOperation(new NewFunctionNode<>(aggregate.getArgs(), StoneDBAggregateFunction.COUNT),
                    new StoneDBCompositeDataType(StoneDBDataType.DECIMAL).getPrimitiveDataType());
            return aliasArgs(Arrays.asList(sum, count));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<Node<StoneDBExpression>> aliasArgs(List<Node<StoneDBExpression>> originalAggregateArgs) {
        List<Node<StoneDBExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<StoneDBExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<StoneDBExpression>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<StoneDBExpression, StoneDBAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
        case AVG:
            return "SUM(agg0)/SUM(agg1)";
        case COUNT:
            return StoneDBAggregateFunction.SUM + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private StoneDBSelect getSelect(List<Node<StoneDBExpression>> aggregates, List<Node<StoneDBExpression>> fromList,
            Node<StoneDBExpression> whereClause, List<Node<StoneDBExpression>> joinList) {
        StoneDBSelect select = new StoneDBSelect();
        select.setFetchColumns(aggregates);
        select.setFromList(fromList);
        select.setWhereClause(whereClause);
        select.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return select;
    }
}
