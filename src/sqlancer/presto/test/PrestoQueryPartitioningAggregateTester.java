package sqlancer.presto.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoAggregateFunction;
import sqlancer.presto.ast.PrestoAlias;
import sqlancer.presto.ast.PrestoCastFunction;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoFunctionNode;
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
        List<PrestoExpression> aggregateArgs = gen.generateArgsForAggregate(aggregateFunction);
        PrestoFunctionNode<PrestoAggregateFunction> aggregate = new PrestoFunctionNode<>(aggregateArgs,
                aggregateFunction);
        select.setFetchColumns(List.of(aggregate));
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            select.setOrderByClauses(gen.generateOrderBys());
//        }
        originalQuery = PrestoToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        firstResultType = getAggregateResultType(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
                "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
        if (firstResultType.equals("BINARY") || firstResultType.equals("ARRAY<BINARY>")
                || firstResultType.equals("ARRAY<ARRAY<BINARY>>")) {
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
            PrestoFunctionNode<PrestoAggregateFunction> aggregate, List<PrestoExpression> from) {
        String metamorphicQuery;
        PrestoExpression whereClause = gen.generatePredicate();
        PrestoExpression negatedClause = new PrestoUnaryPrefixOperation(whereClause,
                PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator.NOT);
        PrestoExpression notNullClause = new PrestoUnaryPostfixOperation(whereClause,
                PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NULL);
        List<PrestoExpression> mappedAggregate = mapped(aggregate);
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

    private List<PrestoExpression> mapped(PrestoFunctionNode<PrestoAggregateFunction> aggregate) {
        PrestoCastFunction count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(List.of(aggregate));
        case AVG:
            PrestoFunctionNode<PrestoAggregateFunction> sum = new PrestoFunctionNode<>(aggregate.getArgs(),
                    PrestoAggregateFunction.SUM);
            count = new PrestoCastFunction(new PrestoFunctionNode<>(aggregate.getArgs(), PrestoAggregateFunction.COUNT),
                    new PrestoCompositeDataType(PrestoDataType.FLOAT, 8, 0));
            return aliasArgs(Arrays.asList(sum, count));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<PrestoExpression> aliasArgs(List<PrestoExpression> originalAggregateArgs) {
        List<PrestoExpression> args = new ArrayList<>();
        int i = 0;
        for (PrestoExpression expr : originalAggregateArgs) {
            args.add(new PrestoAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(PrestoFunctionNode<PrestoAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
        case AVG:
            return "SUM(CAST(agg0 AS DOUBLE))/CAST(SUM(agg1) AS DOUBLE)";
        case COUNT:
            return PrestoAggregateFunction.SUM + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private PrestoSelect getSelect(List<PrestoExpression> aggregates, List<PrestoExpression> from,
            PrestoExpression whereClause, List<PrestoExpression> joinList) {
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
