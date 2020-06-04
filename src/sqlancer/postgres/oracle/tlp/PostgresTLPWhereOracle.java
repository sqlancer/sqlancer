package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresVisitor;

public class PostgresTLPWhereOracle extends PostgresTLPBase {

    public PostgresTLPWhereOracle(PostgresGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        String originalQueryString = PostgresVisitor.asString(select);
        List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
                state.getConnection(), state);

        select.setOrderByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = PostgresVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = PostgresVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = PostgresVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = TestOracle.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
    }
}
