package sqlancer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;

public class StateToReproduce {

    private final List<Query> statements = new ArrayList<>();

    private final String databaseName;

    public String databaseVersion;

    protected long seedValue;

    String exception;

    public OracleRunReproductionState localState;

    public StateToReproduce(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getException() {
        return exception;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDatabaseVersion() {
        return databaseVersion;
    }

    /**
     * Logs the statement string without executing the corresponding statement.
     *
     * @param queryString
     *            the query string to be logged
     */
    public void logStatement(String queryString) {
        if (queryString == null) {
            throw new IllegalArgumentException();
        }
        logStatement(new QueryAdapter(queryString));
    }

    /**
     * Logs the statement without executing it.
     *
     * @param query
     *            the query to be logged
     */
    public void logStatement(Query query) {
        if (query == null) {
            throw new IllegalArgumentException();
        }
        statements.add(query);
    }

    public List<Query> getStatements() {
        return Collections.unmodifiableList(statements);
    }

    public void commentStatements() {
        for (int i = 0; i < statements.size(); i++) {
            Query statement = statements.get(i);
            String queryString = statement.getQueryString();
            String newQueryString = "-- " + queryString;
            statements.set(i, new QueryAdapter(newQueryString));
        }
    }

    public long getSeedValue() {
        return seedValue;
    }

    /**
     * Returns a local state in which a test oracle can save useful information about a single run. If the local state
     * is closed without indicating access to it, the local statements will be added to the global state.
     *
     * @return the local state for logging
     */
    public OracleRunReproductionState getLocalState() {
        return localState;
    }

    /**
     * State information that is logged if the test oracle finds a bug or if an exception is thrown.
     */
    public class OracleRunReproductionState implements Closeable {

        private final List<Query> statements = new ArrayList<>();

        public boolean success;

        public OracleRunReproductionState() {
            StateToReproduce.this.localState = this;
        }

        public void executedWithoutError() {
            this.success = true;
        }

        public void log(String s) {
            statements.add(new QueryAdapter(s));
        }

        @Override
        public void close() {
            if (!success) {
                StateToReproduce.this.statements.addAll(statements);
            }

        }

    }

    public OracleRunReproductionState createLocalState() {
        return new OracleRunReproductionState();
    }

}
