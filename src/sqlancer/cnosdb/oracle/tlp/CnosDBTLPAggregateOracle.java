package sqlancer.cnosdb.oracle.tlp;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBExpectedError;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBVisitor;
import sqlancer.cnosdb.ast.*;
import sqlancer.cnosdb.ast.CnosDBAggregate.CnosDBAggregateFunction;
import sqlancer.cnosdb.ast.CnosDBPostfixOperation.PostfixOperator;
import sqlancer.cnosdb.ast.CnosDBPrefixOperation.PrefixOperator;
import sqlancer.cnosdb.client.CnosDBResultSet;
import sqlancer.cnosdb.query.CnosDBSelectQuery;
import sqlancer.common.oracle.TestOracle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CnosDBTLPAggregateOracle extends CnosDBTLPBase implements TestOracle<CnosDBGlobalState> {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public CnosDBTLPAggregateOracle(CnosDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        super.check();
        aggregateCheck();
    }

    protected void aggregateCheck() {
        // now not support
        // CnosDBAggregateFunction.COUNT
        CnosDBAggregateFunction aggregateFunction = Randomly.fromOptions(CnosDBAggregateFunction.MAX,
                CnosDBAggregateFunction.MIN, CnosDBAggregateFunction.SUM);

        CnosDBAggregate aggregate = gen.generateArgsForAggregate(aggregateFunction.getRandomReturnType(),
                aggregateFunction);
        List<CnosDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(fetchColumns);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        originalQuery = CnosDBVisitor.asString(select);
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
            String assertionMessage = String.format("%s: the results mismatch!\n%s\n%s", this.s.getDatabaseName(),
                    firstQueryString, secondQueryString);
            throw new AssertionError(assertionMessage);
        }
    }

    private String createMetamorphicUnionQuery(CnosDBSelect select, CnosDBAggregate aggregate,
            List<CnosDBExpression> from) {
        String metamorphicQuery;
        CnosDBExpression whereClause = gen.generateExpression(CnosDBDataType.BOOLEAN);
        CnosDBExpression negatedClause = new CnosDBPrefixOperation(whereClause, PrefixOperator.NOT);
        CnosDBExpression notNullClause = new CnosDBPostfixOperation(whereClause, PostfixOperator.IS_NULL);
        List<CnosDBExpression> mappedAggregate = mapped(aggregate);
        CnosDBSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinClauses());
        CnosDBSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinClauses());
        CnosDBSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinClauses());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += CnosDBVisitor.asString(leftSelect) + " UNION ALL " + CnosDBVisitor.asString(middleSelect)
                + " UNION ALL " + CnosDBVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) {
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
        String resultString = null;

        errors.addAll(CnosDBExpectedError.Errors());

        CnosDBSelectQuery q = new CnosDBSelectQuery(queryString, errors);
        try {
            q.executeAndGet(state);
            CnosDBResultSet result = q.getResultSet();

            if (result == null || !result.next()) {
                throw new IgnoreMeException();
            }

            resultString = result.getString(1);

        } catch (Exception e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
        }

        return resultString;
    }

    private List<CnosDBExpression> mapped(CnosDBAggregate aggregate) {
        switch (aggregate.getFunction()) {
        case SUM:
        case MAX:
        case MIN:
            return aliasArgs(List.of(aggregate));
        // now not support
        // case COUNT:
        // case AVG:
        default:
            throw new AssertionError(aggregate.getFunction());
        }
    }

    private List<CnosDBExpression> aliasArgs(List<CnosDBExpression> originalAggregateArgs) {
        List<CnosDBExpression> args = new ArrayList<>();
        int i = 0;
        for (CnosDBExpression expr : originalAggregateArgs) {
            args.add(new CnosDBAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(CnosDBAggregate aggregate) {
        if (Objects.requireNonNull(aggregate.getFunction()) == CnosDBAggregateFunction.COUNT) {
            return CnosDBAggregateFunction.SUM + "(agg0)";
        }
        return aggregate.getFunction() + "(agg0)";
    }

    private CnosDBSelect getSelect(List<CnosDBExpression> aggregates, List<CnosDBExpression> from,
            CnosDBExpression whereClause, List<CnosDBJoin> joinList) {
        CnosDBSelect leftSelect = new CnosDBSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinClauses(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
