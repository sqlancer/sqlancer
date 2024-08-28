package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBAlias;
import sqlancer.duckdb.ast.DuckDBBinaryOperator;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBFunction;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBAggregateFunction;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBBinaryArithmeticOperator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBCastOperation;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBUnaryPostfixOperator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBUnaryPrefixOperator;

public class DuckDBQueryPartitioningAggregateTester extends DuckDBQueryPartitioningBase
        implements TestOracle<DuckDBGlobalState> {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public DuckDBQueryPartitioningAggregateTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        DuckDBAggregateFunction aggregateFunction = Randomly.fromOptions(DuckDBAggregateFunction.MAX,
                DuckDBAggregateFunction.MIN, DuckDBAggregateFunction.SUM, DuckDBAggregateFunction.COUNT,
                DuckDBAggregateFunction.AVG/* , DuckDBAggregateFunction.STDDEV_POP */);
        DuckDBFunction<DuckDBAggregateFunction> aggregate = gen.generateArgsForAggregate(aggregateFunction);
        List<DuckDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        originalQuery = DuckDBToStringVisitor.asString(select);
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

    private String createMetamorphicUnionQuery(DuckDBSelect select, DuckDBFunction<DuckDBAggregateFunction> aggregate,
            List<DuckDBExpression> from) {
        String metamorphicQuery;
        DuckDBExpression whereClause = gen.generateExpression();
        DuckDBExpression negatedClause = new sqlancer.duckdb.ast.DuckDBUnaryPrefixOperator(whereClause,
                DuckDBUnaryPrefixOperator.NOT);
        DuckDBExpression notNullClause = new sqlancer.duckdb.ast.DuckDBUnaryPostfixOperator(whereClause,
                DuckDBUnaryPostfixOperator.IS_NULL);
        List<DuckDBExpression> mappedAggregate = mapped(aggregate);
        DuckDBSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        DuckDBSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        DuckDBSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += DuckDBToStringVisitor.asString(leftSelect) + " UNION ALL "
                + DuckDBToStringVisitor.asString(middleSelect) + " UNION ALL "
                + DuckDBToStringVisitor.asString(rightSelect);
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

    private List<DuckDBExpression> mapped(DuckDBFunction<DuckDBAggregateFunction> aggregate) {
        DuckDBCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(Arrays.asList(aggregate));
        case AVG:
            DuckDBFunction<DuckDBAggregateFunction> sum = new DuckDBFunction<>(aggregate.getArgs(),
                    DuckDBAggregateFunction.SUM);
            count = new DuckDBCastOperation(new DuckDBFunction<>(aggregate.getArgs(), DuckDBAggregateFunction.COUNT),
                    new DuckDBCompositeDataType(DuckDBDataType.FLOAT, 8));
            return aliasArgs(Arrays.asList(sum, count));
        case STDDEV_POP:
            DuckDBFunction<DuckDBAggregateFunction> sumSquared = new DuckDBFunction<>(
                    Arrays.asList(new DuckDBBinaryOperator(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
                            DuckDBBinaryArithmeticOperator.MULT)),
                    DuckDBAggregateFunction.SUM);
            count = new DuckDBCastOperation(new DuckDBFunction<>(aggregate.getArgs(), DuckDBAggregateFunction.COUNT),
                    new DuckDBCompositeDataType(DuckDBDataType.FLOAT, 8));
            DuckDBFunction<DuckDBAggregateFunction> avg = new DuckDBFunction<>(aggregate.getArgs(),
                    DuckDBAggregateFunction.AVG);
            return aliasArgs(Arrays.asList(sumSquared, count, avg));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<DuckDBExpression> aliasArgs(List<DuckDBExpression> originalAggregateArgs) {
        List<DuckDBExpression> args = new ArrayList<>();
        int i = 0;
        for (DuckDBExpression expr : originalAggregateArgs) {
            args.add(new DuckDBAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
        case STDDEV_POP:
            return "sqrt(SUM(agg0)/SUM(agg1)-SUM(agg2)*SUM(agg2))";
        case AVG:
            return "SUM(agg0::FLOAT)/SUM(agg1)::FLOAT";
        case COUNT:
            return DuckDBAggregateFunction.SUM.toString() + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private DuckDBSelect getSelect(List<DuckDBExpression> aggregates, List<DuckDBExpression> from,
            DuckDBExpression whereClause, List<DuckDBExpression> joinList) {
        DuckDBSelect leftSelect = new DuckDBSelect();
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
