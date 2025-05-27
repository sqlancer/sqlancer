package sqlancer.oxla.oracle;

import com.yugabyte.util.PSQLException;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.*;
import sqlancer.oxla.gen.OxlaExpressionGenerator;
import sqlancer.oxla.schema.OxlaDataType;
import sqlancer.oxla.schema.OxlaTables;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OxlaTLPAggregateOracle extends OxlaTLPBase {
    private final static String FORMAT_STRING = "-- %s;\n-- result: %s";

    public OxlaTLPAggregateOracle(OxlaGlobalState state, ExpectedErrors errors) {
        super(state);
        this.errors.addAll(errors);
    }

    @Override
    public void check() throws Exception {
        super.check();
        final OxlaTables targetTables = state.getSchema().getRandomTableNonEmptyTables();
        generator = new OxlaExpressionGenerator(state).setTablesAndColumns(targetTables).setColumns(targetTables.getColumns());
        final OxlaSelect select = new OxlaSelect();
        final OxlaFunctionOperation.OxlaFunction aggregateFunction = Randomly.fromList(OxlaFunctionOperation.AGGREGATE.stream().filter(p -> {
            final String textRepresentation = p.textRepresentation;
            return textRepresentation.equalsIgnoreCase("min") ||
                    textRepresentation.equalsIgnoreCase("max") ||
                    textRepresentation.equalsIgnoreCase("sum") ||
                    textRepresentation.equalsIgnoreCase("count") ||
                    textRepresentation.equalsIgnoreCase("bool_or") ||
                    textRepresentation.equalsIgnoreCase("bool_and");
        }).collect(Collectors.toList()));
        final List<OxlaExpression> arguments = new ArrayList<>();
        for (OxlaDataType type : aggregateFunction.overload.inputTypes) {
            arguments.add(generator.generateExpression(type));
        }
        final OxlaFunctionOperation aggregate = new OxlaFunctionOperation(arguments, aggregateFunction);
        select.setFetchColumns(List.of(aggregate));
        select.setFromList(generator.getTableRefs());
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setJoinClauses(generator.getRandomJoinClauses());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(generator.generateOrderBys());
        }

        final String originalQuery = select.asString();
        final String firstResult = getAggregateResult(originalQuery);
        final String metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        final String secondResult = getAggregateResult(metamorphicQuery);

        String firstQueryString = String.format(FORMAT_STRING, originalQuery, firstResult);
        String secondQueryString = String.format(FORMAT_STRING, metamorphicQuery, secondResult);
        state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
        if (firstResult == null && secondResult != null || firstResult != null && secondResult == null) {
            throw new AssertionError(String.format("[%s] Miss-match between results:\n%s\n%s",
                    state.getDatabaseName(),
                    firstQueryString,
                    secondQueryString));
        }
    }

    private String createMetamorphicUnionQuery(OxlaSelect select, OxlaFunctionOperation aggregate, List<OxlaExpression> fromClauses) {
        final OxlaExpression whereClause = generator.generatePredicate();
        final OxlaExpression negatedClause = new OxlaUnaryPrefixOperation(whereClause, OxlaUnaryPrefixOperation.NOT);
        final OxlaExpression notNullClause = new OxlaUnaryPostfixOperation(whereClause, OxlaUnaryPostfixOperation.IS_NULL);

        final List<OxlaExpression> mappedAggregate = List.of(new OxlaAlias(aggregate, "agg0"));
        OxlaSelect leftSelect = getSelect(mappedAggregate, fromClauses, whereClause, select.getJoinClauses());
        OxlaSelect middleSelect = getSelect(mappedAggregate, fromClauses, negatedClause, select.getJoinClauses());
        OxlaSelect rightSelect = getSelect(mappedAggregate, fromClauses, notNullClause, select.getJoinClauses());

        return String.format("SELECT %s FROM (%s UNION ALL %s UNION ALL %s) as result",
                getOuterAggregateFunction(aggregate),
                leftSelect.asString(),
                middleSelect.asString(),
                rightSelect.asString());
    }

    private OxlaSelect getSelect(List<OxlaExpression> aggregates, List<OxlaExpression> fromClauses, OxlaExpression whereClause, List<OxlaJoin> joinList) {
        final OxlaSelect result = new OxlaSelect();
        result.setFetchColumns(aggregates);
        result.setFromList(fromClauses);
        result.setWhereClause(whereClause);
        result.setJoinClauses(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            result.setGroupByExpressions(generator.generateExpressions(Randomly.smallNumber() + 1));
        }
        return result;
    }

    private String getOuterAggregateFunction(OxlaFunctionOperation aggregate) {
        if (aggregate.getFunc().textRepresentation.equals("count")) {
            return "sum(agg0)";
        }
        return String.format("%s(agg0)", aggregate.getFunc().toString());
    }

    private String getAggregateResult(String queryString) throws SQLException {
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(queryString);
            try {
                state.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        final SQLQueryAdapter query = new SQLQueryAdapter(queryString, errors);
        try (final SQLancerResultSet result = query.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            return result.next() ? result.getString(1) : null;
        } catch (PSQLException e) {
            throw new AssertionError(queryString, e);
        }
    }
}
