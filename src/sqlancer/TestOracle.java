package sqlancer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface TestOracle {

	void check() throws SQLException;

	default boolean onlyWorksForNonEmptyTables() {
		return false;
	}

	static void assumeResultSetsAreEqual(List<String> resultSet, List<String> secondResultSet,
			String originalQueryString, List<String> combinedString, GlobalState<?> state) {
		if (resultSet.size() != secondResultSet.size()) {
			String queryFormatString = "%s; -- cardinality: %d";
			String firstQueryString = String.format(queryFormatString, originalQueryString, resultSet.size());
			String secondQueryString = String.format(queryFormatString,
					combinedString.stream().collect(Collectors.joining(";")), secondResultSet.size());
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
			String secondQueryString = String.format(queryFormatString,
					combinedString.stream().collect(Collectors.joining(";")), secondResultSetMisses);
			state.getState().statements.add(new QueryAdapter(firstQueryString));
			state.getState().statements.add(new QueryAdapter(secondQueryString));
			String assertionMessage = String.format("the content of the result sets mismatch!\n%s\n%s",
					firstQueryString, secondQueryString);
			throw new AssertionError(assertionMessage);
		}
	}

	static List<String> getCombinedResultSet(String firstQueryString, String secondQueryString, String thirdQueryString,
			List<String> combinedString, boolean asUnion, GlobalState<?> state, Set<String> errors)
			throws SQLException {
		List<String> secondResultSet;
		if (asUnion) {
			String unionString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL "
					+ thirdQueryString;
			combinedString.add(unionString);
			secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(unionString, errors,
					state.getConnection(), state);
		} else {
			secondResultSet = new ArrayList<>();
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(firstQueryString, errors,
					state.getConnection(), state));
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(secondQueryString, errors,
					state.getConnection(), state));
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(thirdQueryString, errors,
					state.getConnection(), state));
			combinedString.add(firstQueryString);
			combinedString.add(secondQueryString);
			combinedString.add(thirdQueryString);
		}
		return secondResultSet;
	}

	static List<String> getCombinedResultSetNoDuplicates(String firstQueryString, String secondQueryString,
			String thirdQueryString, List<String> combinedString, boolean asUnion, GlobalState<?> state,
			Set<String> errors) throws SQLException {
		if (!asUnion) {
			throw new AssertionError();
		}
		List<String> secondResultSet;
		String unionString = firstQueryString + " UNION " + secondQueryString + " UNION " + thirdQueryString;
		combinedString.add(unionString);
		secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(unionString, errors, state.getConnection(),
				state);
		return secondResultSet;
	}

}
