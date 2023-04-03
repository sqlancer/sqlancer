package sqlancer.materialize.oracle.tlp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.postgresql.util.PSQLException;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeVisitor;
import sqlancer.materialize.ast.MaterializeAggregate;
import sqlancer.materialize.ast.MaterializeAggregate.MaterializeAggregateFunction;
import sqlancer.materialize.ast.MaterializeAlias;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeJoin;
import sqlancer.materialize.ast.MaterializePostfixOperation;
import sqlancer.materialize.ast.MaterializePostfixOperation.PostfixOperator;
import sqlancer.materialize.ast.MaterializePrefixOperation;
import sqlancer.materialize.ast.MaterializePrefixOperation.PrefixOperator;
import sqlancer.materialize.ast.MaterializeSelect;
import sqlancer.materialize.gen.MaterializeCommon;

public class MaterializeTLPAggregateOracle extends MaterializeTLPBase implements TestOracle<MaterializeGlobalState> {
    private String generatedQueryString;

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public MaterializeTLPAggregateOracle(MaterializeGlobalState state) {
        super(state);
        MaterializeCommon.addGroupingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        aggregateCheck();
    }

    protected void aggregateCheck() throws SQLException {
        MaterializeAggregateFunction aggregateFunction = Randomly.fromOptions(MaterializeAggregateFunction.MAX,
                MaterializeAggregateFunction.MIN, MaterializeAggregateFunction.SUM,
                MaterializeAggregateFunction.BIT_AND, MaterializeAggregateFunction.BIT_OR,
                MaterializeAggregateFunction.BOOL_AND, MaterializeAggregateFunction.BOOL_OR,
                MaterializeAggregateFunction.COUNT);
        MaterializeAggregate aggregate = gen.generateArgsForAggregate(aggregateFunction.getRandomReturnType(),
                aggregateFunction);
        List<MaterializeExpression> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        originalQuery = MaterializeVisitor.asString(select);
        generatedQueryString = originalQuery;
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        String queryFormatString = "-- %s;\n-- result: %s";
        String firstQueryString = String.format(queryFormatString, originalQuery, firstResult);
        String secondQueryString = String.format(queryFormatString, metamorphicQuery, secondResult);
        state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
        if (firstResult == null && secondResult != null || firstResult != null && secondResult == null
                || firstResult != null && !firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult)) {
            if (secondResult != null && secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            String assertionMessage = String.format("the results mismatch!\n%s\n%s", firstQueryString,
                    secondQueryString);
            throw new AssertionError(assertionMessage);
        }
    }

    private String createMetamorphicUnionQuery(MaterializeSelect select, MaterializeAggregate aggregate,
            List<MaterializeExpression> from) {
        String metamorphicQuery;
        MaterializeExpression whereClause = gen.generateExpression(MaterializeDataType.BOOLEAN);
        MaterializeExpression negatedClause = new MaterializePrefixOperation(whereClause, PrefixOperator.NOT);
        MaterializeExpression notNullClause = new MaterializePostfixOperation(whereClause, PostfixOperator.IS_NULL);
        List<MaterializeExpression> mappedAggregate = mapped(aggregate);
        MaterializeSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinClauses());
        MaterializeSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinClauses());
        MaterializeSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinClauses());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += MaterializeVisitor.asString(leftSelect) + " UNION ALL "
                + MaterializeVisitor.asString(middleSelect) + " UNION ALL " + MaterializeVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        // log TLP Aggregate SELECT queries on the current log file
        if (state.getOptions().logEachSelect()) {
            // TODO: refactor me
            state.getLogger().writeCurrent(queryString);
            try {
                state.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
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
        } catch (PSQLException e) {
            throw new AssertionError(queryString, e);
        }
        return resultString;
    }

    private List<MaterializeExpression> mapped(MaterializeAggregate aggregate) {
        switch (aggregate.getFunction()) {
        case SUM:
        case COUNT:
        case BIT_AND:
        case BIT_OR:
        case BOOL_AND:
        case BOOL_OR:
        case MAX:
        case MIN:
            return aliasArgs(Arrays.asList(aggregate));
        default:
            throw new AssertionError(aggregate.getFunction());
        }
    }

    private List<MaterializeExpression> aliasArgs(List<MaterializeExpression> originalAggregateArgs) {
        List<MaterializeExpression> args = new ArrayList<>();
        int i = 0;
        for (MaterializeExpression expr : originalAggregateArgs) {
            args.add(new MaterializeAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(MaterializeAggregate aggregate) {
        switch (aggregate.getFunction()) {
        case COUNT:
            return MaterializeAggregateFunction.SUM.toString() + "(agg0)";
        default:
            return aggregate.getFunction().toString() + "(agg0)";
        }
    }

    private MaterializeSelect getSelect(List<MaterializeExpression> aggregates, List<MaterializeExpression> from,
            MaterializeExpression whereClause, List<MaterializeJoin> joinList) {
        MaterializeSelect leftSelect = new MaterializeSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinClauses(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
