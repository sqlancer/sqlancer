package sqlancer.postgres.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresVisitor;

public class PostgresQueryPartitioningWhereTester extends PostgresQueryPartitioningBase {

	public PostgresQueryPartitioningWhereTester(PostgresGlobalState state) {
		super(state);
	}

	@Override
	public void check() throws SQLException {
		super.check();
		if (Randomly.getBooleanWithRatherLowProbability()) {
			select.setOrderByClause(gen.generateOrderBy());
		}
		String originalQueryString = PostgresVisitor.asString(select);
		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
				state.getConnection());

		select.setOrderByClause(Collections.emptyList());
		select.setWhereClause(predicate);
		String firstQueryString = PostgresVisitor.asString(select);
		select.setWhereClause(negatedPredicate);
		String secondQueryString = PostgresVisitor.asString(select);
		select.setWhereClause(isNullPredicate);
		String thirdQueryString = PostgresVisitor.asString(select);
		List<String> secondResultSet;
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		if (Randomly.getBoolean()) {
			secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(combinedString, errors,
					state.getConnection());
		} else {
			secondResultSet = new ArrayList<>();
			secondResultSet.addAll(
					DatabaseProvider.getResultSetFirstColumnAsString(firstQueryString, errors, state.getConnection()));
			secondResultSet.addAll(
					DatabaseProvider.getResultSetFirstColumnAsString(secondQueryString, errors, state.getConnection()));
			secondResultSet.addAll(
					DatabaseProvider.getResultSetFirstColumnAsString(thirdQueryString, errors, state.getConnection()));
		}
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(originalQueryString);
			state.getLogger().writeCurrent(combinedString);
		}
		if (resultSet.size() != secondResultSet.size()) {
			throw new AssertionError(originalQueryString + ";\n" + combinedString + ";");
		}
	}
}