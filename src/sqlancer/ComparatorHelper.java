package sqlancer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

public final class ComparatorHelper {

    private ComparatorHelper() {
    }

    public static boolean isEqualDouble(String first, String second) {
        try {
            double val = Double.parseDouble(first);
            double secVal = Double.parseDouble(second);
            return equals(val, secVal);
        } catch (Exception e) {
            return false;
        }
    }

    static boolean equals(double a, double b) {
        if (a == b) {
            return true;
        }
        // If the difference is less than epsilon, treat as equal.
        return Math.abs(a - b) < 0.001 * Math.max(Math.abs(a), Math.abs(b)) + 0.001;
    }

    public static List<String> getResultSetFirstColumnAsString(String queryString, ExpectedErrors errors,
            SQLGlobalState<?, ?> state) throws SQLException {
        if (state.getOptions().logEachSelect()) {
            // TODO: refactor me
            state.getLogger().writeCurrent(queryString);
            try {
                state.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        List<String> resultSet = new ArrayList<>();
        SQLancerResultSet result = null;
        try {
            result = q.executeAndGet(state);
            if (result == null) {
                throw new IgnoreMeException();
            }
            while (result.next()) {
                resultSet.add(result.getString(1));
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw e;
            }
            if (e instanceof NumberFormatException) {
                // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/57
                throw new IgnoreMeException();
            }
            if (e.getMessage() == null) {
                throw new AssertionError(queryString, e);
            }
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(queryString, e);
        } finally {
            if (result != null && !result.isClosed()) {
                result.close();
            }
        }
        return resultSet;
    }

    public static void assumeResultSetsAreEqual(List<String> resultSet, List<String> secondResultSet,
            String originalQueryString, List<String> combinedString, SQLGlobalState<?, ?> state) {
        if (resultSet.size() != secondResultSet.size()) {
            String queryFormatString = "-- %s;\n-- cardinality: %d";
            String firstQueryString = String.format(queryFormatString, originalQueryString, resultSet.size());
            String secondQueryString = String.format(queryFormatString,
                    combinedString.stream().collect(Collectors.joining(";")), secondResultSet.size());
            state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
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
            String queryFormatString = "-- %s;\n-- misses: %s";
            String firstQueryString = String.format(queryFormatString, originalQueryString, firstResultSetMisses);
            String secondQueryString = String.format(queryFormatString,
                    combinedString.stream().collect(Collectors.joining(";")), secondResultSetMisses);
            // update the SELECT queries to be logged at the bottom of the error log file
            state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
            String assertionMessage = String.format("the content of the result sets mismatch!\n%s\n%s",
                    firstQueryString, secondQueryString);
            throw new AssertionError(assertionMessage);
        }
    }

    public static List<String> getCombinedResultSet(String firstQueryString, String secondQueryString,
            String thirdQueryString, List<String> combinedString, boolean asUnion, SQLGlobalState<?, ?> state,
            ExpectedErrors errors) throws SQLException {
        List<String> secondResultSet;
        if (asUnion) {
            String unionString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL "
                    + thirdQueryString;
            combinedString.add(unionString);
            secondResultSet = getResultSetFirstColumnAsString(unionString, errors, state);
        } else {
            secondResultSet = new ArrayList<>();
            secondResultSet.addAll(getResultSetFirstColumnAsString(firstQueryString, errors, state));
            secondResultSet.addAll(getResultSetFirstColumnAsString(secondQueryString, errors, state));
            secondResultSet.addAll(getResultSetFirstColumnAsString(thirdQueryString, errors, state));
            combinedString.add(firstQueryString);
            combinedString.add(secondQueryString);
            combinedString.add(thirdQueryString);
        }
        return secondResultSet;
    }

    public static List<String> getCombinedResultSetNoDuplicates(String firstQueryString, String secondQueryString,
            String thirdQueryString, List<String> combinedString, boolean asUnion, SQLGlobalState<?, ?> state,
            ExpectedErrors errors) throws SQLException {
        String unionString;
        if (asUnion) {
            unionString = firstQueryString + " UNION " + secondQueryString + " UNION " + thirdQueryString;
        } else {
            unionString = "SELECT DISTINCT * FROM (" + firstQueryString + " UNION ALL " + secondQueryString
                    + " UNION ALL " + thirdQueryString + ")";
        }
        List<String> secondResultSet;
        combinedString.add(unionString);
        secondResultSet = getResultSetFirstColumnAsString(unionString, errors, state);
        return secondResultSet;
    }

}
