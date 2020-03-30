package sqlancer.tidb.test;

import java.sql.SQLException;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBQueryPartitioningHavingTester extends TiDBQueryPartitioningBase implements TestOracle {

	public TiDBQueryPartitioningHavingTester(TiDBGlobalState state) {
		super(state);
		TiDBErrors.addExpressionHavingErrors(errors);
	}

	@Override
	public void check() throws SQLException {
		// TODO order by?
		if (Randomly.getBoolean()) {
			select.setWhereClause(gen.generateExpression());
		}
		select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
		select.setHavingClause(null);
		String originalQueryString = TiDBVisitor.asString(select);
		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
				state.getConnection(), state);

		select.setHavingClause(predicate);
		String firstQueryString = TiDBVisitor.asString(select);
		select.setHavingClause(negatedPredicate);
		String secondQueryString = TiDBVisitor.asString(select);
		select.setHavingClause(isNullPredicate);
		String thirdQueryString = TiDBVisitor.asString(select);
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		List<String> secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(combinedString, errors,
				state.getConnection(), state);
		TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
	}

	@Override
	TiDBExpression generatePredicate() {
		return gen.generateHavingClause();
	}
}
