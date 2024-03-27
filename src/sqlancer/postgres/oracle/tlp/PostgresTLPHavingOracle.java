package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.oracle.TLPHavingOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresTLPHavingOracle extends PostgresTLPBase {

    private final TLPHavingOracle<PostgresJoin, PostgresExpression, PostgresSchema, PostgresTable, PostgresColumn, PostgresGlobalState> oracle;

    public PostgresTLPHavingOracle(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addGroupingErrors(errors);

        ExpectedErrors expectedErrors = ExpectedErrors.newErrors()
                .with(PostgresCommon.getCommonExpressionErrors().toArray(new String[0]))
                .with(PostgresCommon.getCommonFetchErrors().toArray(new String[0]))
                .with(PostgresCommon.getGroupingErrors().toArray(new String[0]))
                .with(PostgresCommon.getCommonExpressionRegexErrors().toArray(new Pattern[0])).build();

        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(state);
        this.oracle = new TLPHavingOracle<>(state, gen, expectedErrors, true, false);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    protected void havingCheck() throws SQLException {
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateBooleanExpression());
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = PostgresVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByClauses(gen.generateOrderBy());
        }
        select.setHavingClause(predicate);
        String firstQueryString = PostgresVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = PostgresVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = PostgresVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected PostgresExpression generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<PostgresExpression> generateFetchColumns() {
        List<PostgresExpression> expressions = gen.allowAggregates(true)
                .generateExpressions(Randomly.smallNumber() + 1);
        gen.allowAggregates(false);
        return expressions;
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
