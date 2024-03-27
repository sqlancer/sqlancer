package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
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

public class PostgresTLPWhereOracle extends PostgresTLPBase implements TestOracle<PostgresGlobalState> {

    private final TLPWhereOracle<PostgresJoin, PostgresExpression, PostgresSchema, PostgresTable, PostgresColumn, PostgresGlobalState> oracle;

    public PostgresTLPWhereOracle(PostgresGlobalState state) {
        super(state); // TODO remove after updating citus
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors()
                .with(PostgresCommon.getCommonExpressionErrors().toArray(new String[0]))
                .with(PostgresCommon.getCommonFetchErrors().toArray(new String[0]))
                .with(PostgresCommon.getCommonExpressionRegexErrors().toArray(new Pattern[0])).build();

        this.oracle = new TLPWhereOracle<>(state, gen, expectedErrors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    // TODO remove after updating citus
    protected void whereCheck() throws SQLException {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBy());
        }
        String originalQueryString = PostgresVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setOrderByClauses(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = PostgresVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = PostgresVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = PostgresVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }

}
