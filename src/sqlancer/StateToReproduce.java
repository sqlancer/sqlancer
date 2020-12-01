package sqlancer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.common.query.Query;

public class StateToReproduce {

    private final List<Query<?>> statements = new ArrayList<>();

    private final String databaseName;

    private final DatabaseProvider<?, ?, ?> databaseProvider;

    public String databaseVersion;

    protected long seedValue;

    String exception;

    public OracleRunReproductionState localState;

    public StateToReproduce(String databaseName, DatabaseProvider<?, ?, ?> databaseProvider) {
        this.databaseName = databaseName;
        this.databaseProvider = databaseProvider;
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
        logStatement(databaseProvider.getLoggableFactory().getQueryForStateToReproduce(queryString));
    }

    /**
     * Logs the statement without executing it.
     *
     * @param query
     *            the query to be logged
     */
    public void logStatement(Query<?> query) {
        if (query == null) {
            throw new IllegalArgumentException();
        }
        statements.add(query);
    }

    public List<Query<?>> getStatements() {
        return Collections.unmodifiableList(statements);
    }

    @Deprecated
    public void commentStatements() {
        for (int i = 0; i < statements.size(); i++) {
            Query<?> statement = statements.get(i);
            Query<?> newQuery = databaseProvider.getLoggableFactory().commentOutQuery(statement);
            statements.set(i, newQuery);
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

        private final List<Query<?>> statements = new ArrayList<>();

        public boolean success;

        public OracleRunReproductionState() {
            StateToReproduce.this.localState = this;
        }

        public void executedWithoutError() {
            this.success = true;
        }

        public void log(String s) {
            statements.add(databaseProvider.getLoggableFactory().getQueryForStateToReproduce(s));
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
