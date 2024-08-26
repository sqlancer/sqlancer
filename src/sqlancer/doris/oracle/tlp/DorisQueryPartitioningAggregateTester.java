package sqlancer.doris.oracle.tlp;

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
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisCompositeDataType;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.ast.DorisAggregateOperation;
import sqlancer.doris.ast.DorisAlias;
import sqlancer.doris.ast.DorisAggregateOperation.DorisAggregateFunction;
import sqlancer.doris.ast.DorisBinaryArithmeticOperation;
import sqlancer.doris.ast.DorisBinaryOperation;
import sqlancer.doris.ast.DorisCastOperation;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisFunction;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.ast.DorisUnaryPostfixOperation;
import sqlancer.doris.ast.DorisUnaryPrefixOperation;
import sqlancer.doris.ast.DorisUnaryPostfixOperation.DorisUnaryPostfixOperator;
import sqlancer.doris.ast.DorisUnaryPrefixOperation.DorisUnaryPrefixOperator;
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
        DorisFunction<DorisAggregateFunction> aggregate = (DorisAggregateOperation) gen
                .generateArgsForAggregate(aggregateFunction);
        List<DorisExpression> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add((DorisAggregateOperation) gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<DorisExpression> constants = new ArrayList<>();
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
        if (firstResult == null && secondResult == null) {
            return;
        }
        if (firstResult == null) {
            throw new AssertionError();
        }
        firstResult = firstResult.replace("\0", "");
        if (firstResult.contentEquals("0") && secondResult == null) {
            return;
        }
        if (secondResult == null) {
            throw new AssertionError();
        }
        secondResult = secondResult.replace("\0", "");
        if (!firstResult.contentEquals(secondResult) && !ComparatorHelper.isEqualDouble(firstResult, secondResult)) {
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(DorisSelect select, DorisFunction<DorisAggregateFunction> aggregate,
            List<DorisExpression> from) {
        String metamorphicQuery;
        DorisExpression whereClause = gen.generateExpression(DorisDataType.BOOLEAN);
        DorisExpression negatedClause = new DorisUnaryPrefixOperation(whereClause, DorisUnaryPrefixOperator.NOT);
        DorisExpression notNullClause = new DorisUnaryPostfixOperation(whereClause, DorisUnaryPostfixOperator.IS_NULL);
        List<DorisExpression> mappedAggregate = mapped(aggregate);
        DorisSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        DorisSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        DorisSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(groupByExpression);
            middleSelect.setGroupByExpressions(groupByExpression);
            rightSelect.setGroupByExpressions(groupByExpression);
        }
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

    private List<DorisExpression> mapped(DorisFunction<DorisAggregateFunction> aggregate) {

        DorisCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(Arrays.asList(aggregate));
        case AVG:
            DorisFunction<DorisAggregateFunction> sum = new DorisFunction<>(aggregate.getArgs(),
                    DorisAggregateFunction.SUM);
            count = new DorisCastOperation(new DorisFunction<>(aggregate.getArgs(), DorisAggregateFunction.COUNT),
                    new DorisCompositeDataType(DorisDataType.FLOAT, 8));
            return aliasArgs(Arrays.asList(sum, count));
        case STDDEV_POP:
            DorisFunction<DorisAggregateFunction> sumSquared = new DorisFunction<>(
                    Arrays.asList(new DorisBinaryOperation(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
                            DorisBinaryArithmeticOperation.DorisBinaryArithmeticOperator.MULTIPLICATION)),
                    DorisAggregateFunction.SUM);
            count = new DorisCastOperation(new DorisFunction<>(aggregate.getArgs(), DorisAggregateFunction.COUNT),
                    new DorisCompositeDataType(DorisDataType.FLOAT, 8));
            DorisFunction<DorisAggregateFunction> avg = new DorisFunction<>(aggregate.getArgs(),
                    DorisAggregateFunction.AVG);
            return aliasArgs(Arrays.asList(sumSquared, count, avg));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<DorisExpression> aliasArgs(List<DorisExpression> originalAggregateArgs) {
        List<DorisExpression> args = new ArrayList<>();
        int i = 0;
        for (DorisExpression expr : originalAggregateArgs) {
            args.add(new DorisAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(DorisFunction<DorisAggregateFunction> aggregate) {
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

    private DorisSelect getSelect(List<DorisExpression> aggregates, List<DorisExpression> from,
            DorisExpression whereClause, List<DorisExpression> joinList) {
        DorisSelect leftSelect = new DorisSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinList(joinList);
        return leftSelect;
    }

}
