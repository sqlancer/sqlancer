package sqlancer.databend.test.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendAggregateOperation;
import sqlancer.databend.ast.DatabendAggregateOperation.DatabendAggregateFunction;
import sqlancer.databend.ast.DatabendAlias;
import sqlancer.databend.ast.DatabendBinaryArithmeticOperation.DatabendBinaryArithmeticOperator;
import sqlancer.databend.ast.DatabendBinaryOperation;
import sqlancer.databend.ast.DatabendCastOperation;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendFunctionOperation;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.ast.DatabendUnaryPostfixOperation;
import sqlancer.databend.ast.DatabendUnaryPostfixOperation.DatabendUnaryPostfixOperator;
import sqlancer.databend.ast.DatabendUnaryPrefixOperation;
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
        DatabendFunctionOperation<DatabendAggregateFunction> aggregate = (DatabendAggregateOperation) gen
                .generateArgsForAggregate(aggregateFunction);
        List<DatabendExpression> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add((DatabendAggregateOperation) gen.generateAggregate()); // TODO 更换成非聚合函数
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        // if (Randomly.getBooleanWithRatherLowProbability()) {
        // select.setOrderByClauses(gen.generateOrderBys());
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
            DatabendFunctionOperation<DatabendAggregateFunction> aggregate, List<DatabendExpression> from) {
        String metamorphicQuery;
        DatabendExpression whereClause = gen.generateExpression(DatabendDataType.BOOLEAN);
        DatabendExpression negatedClause = new DatabendUnaryPrefixOperation(whereClause,
                DatabendUnaryPrefixOperator.NOT);
        DatabendExpression notNullClause = new DatabendUnaryPostfixOperation(whereClause,
                DatabendUnaryPostfixOperator.IS_NULL);
        List<DatabendExpression> mappedAggregate = mapped(aggregate);
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

    private List<DatabendExpression> mapped(DatabendFunctionOperation<DatabendAggregateFunction> aggregate) {
        DatabendCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(Arrays.asList(aggregate));
        case AVG:
            DatabendFunctionOperation<DatabendAggregateFunction> sum = new DatabendFunctionOperation<>(
                    aggregate.getArgs(), DatabendAggregateFunction.SUM);
            count = new DatabendCastOperation(
                    new DatabendFunctionOperation<>(aggregate.getArgs(), DatabendAggregateFunction.COUNT),
                    new DatabendCompositeDataType(DatabendDataType.FLOAT, 8));
            return aliasArgs(Arrays.asList(sum, count));
        case STDDEV_POP:
            DatabendFunctionOperation<DatabendAggregateFunction> sumSquared = new DatabendFunctionOperation<>(
                    Arrays.asList(new DatabendBinaryOperation(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
                            DatabendBinaryArithmeticOperator.MULTIPLICATION)),
                    DatabendAggregateFunction.SUM);
            count = new DatabendCastOperation(
                    new DatabendFunctionOperation<>(aggregate.getArgs(), DatabendAggregateFunction.COUNT),
                    new DatabendCompositeDataType(DatabendDataType.FLOAT, 8));
            DatabendFunctionOperation<DatabendAggregateFunction> avg = new DatabendFunctionOperation<>(
                    aggregate.getArgs(), DatabendAggregateFunction.AVG);
            return aliasArgs(Arrays.asList(sumSquared, count, avg));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<DatabendExpression> aliasArgs(List<DatabendExpression> originalAggregateArgs) {
        List<DatabendExpression> args = new ArrayList<>();
        int i = 0;
        for (DatabendExpression expr : originalAggregateArgs) {
            args.add(new DatabendAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(DatabendFunctionOperation<DatabendAggregateFunction> aggregate) {
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

    private DatabendSelect getSelect(List<DatabendExpression> aggregates, List<DatabendExpression> from,
            DatabendExpression whereClause, List<DatabendExpression> joinList) {
        DatabendSelect select = new DatabendSelect();
        select.setFetchColumns(aggregates);
        select.setFromList(from);
        select.setWhereClause(whereClause);
        select.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setGroupByExpressions(List.of(gen.generateConstant(DatabendDataType.INT))); // TODO
                                                                                               // 仍可加强
        }
        return select;
    }

}
