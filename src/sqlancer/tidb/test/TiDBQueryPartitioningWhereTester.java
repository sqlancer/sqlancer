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

public class TiDBQueryPartitioningWhereTester extends TiDBQueryPartitioningBase {

	public TiDBQueryPartitioningWhereTester(TiDBGlobalState state) {
		super(state);
	}

	@Override
	public void check() throws SQLException {
		super.check();
		select.setWhereClause(null);
		String originalQueryString = TiDBVisitor.asString(select);

		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
				state.getConnection(), state);

		if (Randomly.getBoolean()) {
			select.setOrderByExpressions(gen.generateOrderBys());
		}
		select.setOrderByExpressions(Collections.emptyList());
		select.setWhereClause(predicate);
		String firstQueryString = TiDBVisitor.asString(select);
		select.setWhereClause(negatedPredicate);
		String secondQueryString = TiDBVisitor.asString(select);
		select.setWhereClause(isNullPredicate);
		String thirdQueryString = TiDBVisitor.asString(select);
		List<String> combinedString = new ArrayList<>();
		List<String> secondResultSet = TestOracle.getCombinedResultSet(firstQueryString, secondQueryString,
				thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
		TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
	}

}