package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresCommon;

public class PostgresTLPHavingOracle extends PostgresTLPBase {
    private String generatedQueryString;

    public PostgresTLPHavingOracle(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addGroupingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        havingCheck();
    }

    protected void havingCheck() throws SQLException {
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(PostgresDataType.BOOLEAN));
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = PostgresVisitor.asString(select);
        generatedQueryString = originalQueryString;
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // Mz: See #18346, have to check if predicate errors by putting it in SELECT first
        List<PostgresExpression> originalColumns = select.getFetchColumns();
        List<PostgresExpression> checkColumns = new ArrayList<>();
        checkColumns.add(predicate);
        select.setFetchColumns(checkColumns);
        String errorCheckQueryString = PostgresVisitor.asString(select);
        ComparatorHelper.getResultSetFirstColumnAsString(errorCheckQueryString, errors, state);
        select.setFetchColumns(originalColumns);

        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBy());
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
        return generatedQueryString;
    }
}
