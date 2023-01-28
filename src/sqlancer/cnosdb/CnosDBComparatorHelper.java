package sqlancer.cnosdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.cnosdb.client.CnosDBResultSet;
import sqlancer.cnosdb.query.CnosDBSelectQuery;
import sqlancer.common.query.ExpectedErrors;

public final class CnosDBComparatorHelper {

    private CnosDBComparatorHelper() {
    }

    public static List<String> getResultSetFirstColumnAsString(String queryString, ExpectedErrors errors,
            CnosDBGlobalState state) throws Exception {
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
        CnosDBSelectQuery q = new CnosDBSelectQuery(queryString, errors);
        List<String> result = new ArrayList<>();
        CnosDBResultSet resultSet;
        try {
            q.executeAndGet(state);
            resultSet = q.getResultSet();
            if (resultSet == null) {
                throw new AssertionError(q);
            }
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw e;
            }
            if (e instanceof NumberFormatException) {
                throw new IgnoreMeException();
            }
            if (e.getMessage() == null) {
                throw new AssertionError(queryString, e);
            }
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(queryString, e);
        }

        return result;
    }

    public static void assumeResultSetsAreEqual(List<String> resultSet, List<String> secondResultSet,
            String originalQueryString, List<String> combinedString, CnosDBGlobalState state) {
        if (resultSet.size() != secondResultSet.size()) {
            String queryFormatString = "-- %s;\n-- cardinality: %d";
            String firstQueryString = String.format(queryFormatString, originalQueryString, resultSet.size());
            String secondQueryString = String.format(queryFormatString, String.join(";", combinedString),
                    secondResultSet.size());
            state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
            String assertionMessage = String.format("the size of the result sets mismatch (%d and %d)!\n%s\n%s",
                    resultSet.size(), secondResultSet.size(), firstQueryString, secondQueryString);
            Main.nrUnsuccessfulActions.addAndGet(1);
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
            String secondQueryString = String.format(queryFormatString, String.join(";", combinedString),
                    secondResultSetMisses);
            // update the SELECT queries to be logged at the bottom of the error log file
            state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
            String assertionMessage = String.format("the content of the result sets mismatch!\n%s\n%s",
                    firstQueryString, secondQueryString);
            Main.nrUnsuccessfulActions.addAndGet(1);
            throw new AssertionError(assertionMessage);
        }
        Main.nrSuccessfulActions.addAndGet(1);
    }

    public static void assumeResultSetsAreEqual(List<String> resultSet, List<String> secondResultSet,
            String originalQueryString, List<String> combinedString, CnosDBGlobalState state,
            UnaryOperator<String> canonicalizationRule) {
        // Overloaded version of assumeResultSetsAreEqual that takes a canonicalization function which is applied to
        // both result sets before their comparison.
        List<String> canonicalizedResultSet = resultSet.stream().map(canonicalizationRule).collect(Collectors.toList());
        List<String> canonicalizedSecondResultSet = secondResultSet.stream().map(canonicalizationRule)
                .collect(Collectors.toList());
        assumeResultSetsAreEqual(canonicalizedResultSet, canonicalizedSecondResultSet, originalQueryString,
                combinedString, state);
    }

    public static List<String> getCombinedResultSet(String firstQueryString, String secondQueryString,
            String thirdQueryString, List<String> combinedString, boolean asUnion, CnosDBGlobalState state,
            ExpectedErrors errors) throws Exception {
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
            String thirdQueryString, List<String> combinedString, boolean asUnion, CnosDBGlobalState state,
            ExpectedErrors errors) throws Exception {
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
