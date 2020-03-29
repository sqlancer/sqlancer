package sqlancer.postgres.test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresCommon;

public class PostgresQueryPartitioningHavingTester extends PostgresQueryPartitioningBase {

	public PostgresQueryPartitioningHavingTester(PostgresGlobalState state) {
		super(state);
		PostgresCommon.addGroupingErrors(errors);
	}

	@Override
	public void check() throws SQLException {
		super.check();
		if (Randomly.getBooleanWithRatherLowProbability()) {
			select.setOrderByExpressions(gen.generateOrderBy());
		}
		if (Randomly.getBoolean()) {
			select.setWhereClause(gen.generateExpression(PostgresDataType.BOOLEAN));
		}
		select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
		select.setHavingClause(null);
		String originalQueryString = PostgresVisitor.asString(select);
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(originalQueryString);
			try {
				state.getLogger().getCurrentFileWriter().flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
				state.getConnection());

		select.setOrderByExpressions(Collections.emptyList()); // not compatible with the union
		select.setHavingClause(predicate);
		String firstQueryString = PostgresVisitor.asString(select);
		select.setHavingClause(negatedPredicate);
		String secondQueryString = PostgresVisitor.asString(select);
		select.setHavingClause(isNullPredicate);
		String thirdQueryString = PostgresVisitor.asString(select);
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(combinedString);
			try {
				state.getLogger().getCurrentFileWriter().flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		List<String> secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(combinedString, errors,
				state.getConnection());
		if (resultSet.size() != secondResultSet.size()) {
			throw new AssertionError(originalQueryString + ";\n" + combinedString + ";");
		}
	}

	@Override
	PostgresExpression generatePredicate() {
		return gen.generateHavingClause();
	}

	@Override
	List<PostgresExpression> generateFetchColumns() {
		List<PostgresExpression> expressions = gen.allowAggregates(true)
				.generateExpressions(Randomly.smallNumber() + 1);
		gen.allowAggregates(false);
		return expressions;
	}

}
