package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBQueryPartitioningDistinctTester extends DuckDBQueryPartitioningBase {

	public DuckDBQueryPartitioningDistinctTester(DuckDBGlobalState state) {
		super(state);
		DuckDBErrors.addGroupByErrors(errors);
	}

	@Override
	public void check() throws SQLException {
		super.check();
		select.setDistinct(true);
		select.setWhereClause(null);
		String originalQueryString = DuckDBToStringVisitor.asString(select);

		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
				state.getConnection(), state);
		if (Randomly.getBoolean()) {
			select.setDistinct(false);
		}
		select.setWhereClause(predicate);
		String firstQueryString = DuckDBToStringVisitor.asString(select);
		select.setWhereClause(negatedPredicate);
		String secondQueryString = DuckDBToStringVisitor.asString(select);
		select.setWhereClause(isNullPredicate);
		String thirdQueryString = DuckDBToStringVisitor.asString(select);
		List<String> combinedString = new ArrayList<>();
		List<String> secondResultSet = TestOracle.getCombinedResultSetNoDuplicates(firstQueryString, secondQueryString,
				thirdQueryString, combinedString, true, state, errors);
		TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
	}

}
