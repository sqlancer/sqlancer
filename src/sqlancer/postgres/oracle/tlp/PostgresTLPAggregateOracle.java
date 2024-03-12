package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TLPAggregateOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresAggregate;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresTLPAggregateOracle extends PostgresTLPBase implements TestOracle<PostgresGlobalState> {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    private final TLPAggregateOracle<PostgresAggregate, PostgresExpression, PostgresSchema, PostgresTable, PostgresColumn, PostgresGlobalState> oracle;

    public PostgresTLPAggregateOracle(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addGroupingErrors(errors);

        ExpectedErrors expectedErrors = ExpectedErrors.newErrors()
                .with(PostgresCommon.getCommonExpressionErrors().toArray(new String[0]))
                .with(PostgresCommon.getCommonFetchErrors().toArray(new String[0]))
                .with(PostgresCommon.getGroupingErrors().toArray(new String[0]))
                .with(PostgresCommon.getCommonExpressionRegexErrors().toArray(new Pattern[0])).build();

        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(state);
        this.oracle = new TLPAggregateOracle<>(state, gen, expectedErrors, false);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }

    protected void aggregateCheck() throws SQLException {
        PostgresAggregate aggregate = gen.generateAggregate();
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBy());
        }
        originalQuery = PostgresVisitor.asString(select);
        firstResult = ComparatorHelper.runQuery(originalQuery, errors, state);

        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = ComparatorHelper.runQuery(metamorphicQuery, errors, state);

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

    private String createMetamorphicUnionQuery(PostgresSelect select, PostgresAggregate aggregate,
            List<PostgresExpression> from) {
        PostgresExpression whereClause = gen.generateExpression(PostgresDataType.BOOLEAN);
        PostgresExpression negatedClause = gen.negatePredicate(whereClause);
        PostgresExpression notNullClause = gen.isNull(whereClause);
        PostgresSelect leftSelect = getSelect(aggregate, from, whereClause, select.getJoinClauses());
        PostgresSelect middleSelect = getSelect(aggregate, from, negatedClause, select.getJoinClauses());
        PostgresSelect rightSelect = getSelect(aggregate, from, notNullClause, select.getJoinClauses());
        return aggregate.asAggregatedString(leftSelect.asString(), middleSelect.asString(), rightSelect.asString());
    }

    private PostgresSelect getSelect(PostgresAggregate aggregate, List<PostgresExpression> from,
            PostgresExpression whereClause, List<PostgresJoin> joinList) {
        PostgresSelect leftSelect = new PostgresSelect();
        leftSelect.setFetchColumns(gen.aliasAggregates(List.of(aggregate)));
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinClauses(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
