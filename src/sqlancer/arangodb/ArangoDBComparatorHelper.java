package sqlancer.arangodb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.arangodb.entity.BaseDocument;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.arangodb.query.ArangoDBSelectQuery;
import sqlancer.common.query.ExpectedErrors;

public final class ArangoDBComparatorHelper {

    private ArangoDBComparatorHelper() {

    }

    public static List<BaseDocument> getResultSetAsDocumentList(ArangoDBSelectQuery query,
            ArangoDBProvider.ArangoDBGlobalState state) throws Exception {
        ExpectedErrors errors = query.getExpectedErrors();
        List<BaseDocument> result;
        try {
            query.executeAndGet(state);
            Main.nrSuccessfulActions.addAndGet(1);
            result = query.getResultSet();
            return result;
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw e;
            }
            Main.nrUnsuccessfulActions.addAndGet(1);
            if (e.getMessage() == null) {
                throw new AssertionError(query.getLogString(), e);
            }
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(query.getLogString(), e);
        }

    }

    public static void assumeResultSetsAreEqual(List<BaseDocument> resultSet, List<BaseDocument> secondResultSet,
            ArangoDBSelectQuery originalQuery) {
        if (resultSet.size() != secondResultSet.size()) {
            String assertionMessage = String.format("The Size of the result sets mismatch (%d and %d)!\n%s",
                    resultSet.size(), secondResultSet.size(), originalQuery.getLogString());
            throw new AssertionError(assertionMessage);
        }
        Set<BaseDocument> firstHashSet = new HashSet<>(resultSet);
        Set<BaseDocument> secondHashSet = new HashSet<>(secondResultSet);

        if (!firstHashSet.equals(secondHashSet)) {
            Set<BaseDocument> firstResultSetMisses = new HashSet<>(firstHashSet);
            firstResultSetMisses.removeAll(secondHashSet);
            Set<BaseDocument> secondResultSetMisses = new HashSet<>(secondHashSet);
            secondResultSetMisses.removeAll(firstHashSet);
            StringBuilder firstMisses = new StringBuilder();
            for (BaseDocument document : firstResultSetMisses) {
                firstMisses.append(document).append(" ");
            }
            StringBuilder secondMisses = new StringBuilder();
            for (BaseDocument document : secondResultSetMisses) {
                secondMisses.append(document).append(" ");
            }
            String assertMessage = String.format("The Content of the result sets mismatch!\n %s \n %s\n %s",
                    firstMisses.toString(), secondMisses.toString(), originalQuery.getLogString());
            throw new AssertionError(assertMessage);
        }
    }
}
