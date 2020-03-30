package sqlancer.tidb.test;

import java.sql.SQLException;
import java.util.ArrayList;
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
		if (Randomly.getBoolean()) {
			select.setWhereClause(gen.generateExpression());
		}
		if (Randomly.getBoolean()) {
			select.setOrderByExpressions(gen.generateOrderBys());
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
		List<String> combinedString = new ArrayList<>();
		List<String> secondResultSet = TestOracle.getCombinedResultSet(firstQueryString, secondQueryString,
				thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
		TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
	}

	@Override
	TiDBExpression generatePredicate() {
		return gen.generateHavingClause();
	}
}
