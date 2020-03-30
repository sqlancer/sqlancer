package sqlancer;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public interface TestOracle {

	public void check() throws SQLException;

	public default boolean onlyWorksForNonEmptyTables() {
		return false;
	}

	public static void assumeResultSetsAreEqual(List<String> resultSet, List<String> secondResultSet,
			String originalQueryString, String combinedString, GlobalState state) {
		if (resultSet.size() != secondResultSet.size()) {
			String queryFormatString = "%s; -- cardinality: %d";
			String firstQueryString = String.format(queryFormatString, originalQueryString, resultSet.size());
			String secondQueryString = String.format(queryFormatString, combinedString, secondResultSet.size());
			state.getState().statements.add(new QueryAdapter(firstQueryString));
			state.getState().statements.add(new QueryAdapter(secondQueryString));
			String assertionMessage = String.format("the size of the result sets mismatch (%d and %d)!\n%s\n%s",
					resultSet.size(), secondResultSet.size(), firstQueryString, secondQueryString);
			throw new AssertionError(assertionMessage);
		}

		Set<String> firstHashSet = new HashSet<>(resultSet);
		Set<String> secondHashSet = new HashSet<>(secondResultSet);

		if (!firstHashSet.equals(secondHashSet)) {
			Set<String> firstResultSetMisses = new HashSet<>(firstHashSet);
			firstResultSetMisses.removeAll(secondHashSet);
			Set<String> secondResultSetMisses = new HashSet<>(secondHashSet);
			secondResultSetMisses.removeAll(firstHashSet);
			String queryFormatString = "%s; -- misses: %s";
			String firstQueryString = String.format(queryFormatString, originalQueryString, firstResultSetMisses);
			String secondQueryString = String.format(queryFormatString, combinedString, secondResultSetMisses);
			state.getState().statements.add(new QueryAdapter(firstQueryString));
			state.getState().statements.add(new QueryAdapter(secondQueryString));
			String assertionMessage = String.format("the content of the result sets mismatch!\n%s\n%s",
					firstQueryString, secondQueryString);
			throw new AssertionError(assertionMessage);
		}
	}

}
