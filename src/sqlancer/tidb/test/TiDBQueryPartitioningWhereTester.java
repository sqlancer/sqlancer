package sqlancer.tidb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBQueryPartitioningWhereTester extends TiDBQueryPartitioningBase  {


	public TiDBQueryPartitioningWhereTester(TiDBGlobalState state) {
		super(state);
	}

	@Override
	public void check() throws SQLException {
		super.check();
		select.setWhereClause(null);
		if (Randomly.getBoolean()) {
			select.setOrderByExpressions(gen.generateOrderBys());
		}
		String originalQueryString = TiDBVisitor.asString(select);
		
		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors, state.getConnection());
		
		select.setOrderByExpressions(Collections.emptyList());
		select.setWhereClause(predicate);
		String firstQueryString = TiDBVisitor.asString(select);
		select.setWhereClause(negatedPredicate);
		String secondQueryString = TiDBVisitor.asString(select);
		select.setWhereClause(isNullPredicate);
		String thirdQueryString = TiDBVisitor.asString(select);
		List<String> secondResultSet;
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		if (Randomly.getBoolean()) {
			secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(combinedString, errors, state.getConnection());
		} else {
			secondResultSet = new ArrayList<>();
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(firstQueryString, errors, state.getConnection()));
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(secondQueryString, errors, state.getConnection()));
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(thirdQueryString, errors, state.getConnection()));
		}
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(originalQueryString);
			state.getLogger().writeCurrent(combinedString);
		}
		TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
	}
}