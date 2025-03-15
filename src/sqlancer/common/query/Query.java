package sqlancer.common.query;

import sqlancer.GlobalState;
import sqlancer.SQLancerDBConnection;
import sqlancer.common.log.Loggable;

public abstract class Query<C extends SQLancerDBConnection> implements Loggable {

    /**
     * Gets the query string, which is guaranteed to be terminated with a semicolon.
     *
     * @return the query string.
     */
    public abstract String getQueryString();

    /**
     * Gets the query string without trailing semicolons.
     *
     * @return the query string that does not end with a ";".
     */
    public abstract String getUnterminatedQueryString();

    /**
     * Whether the query could affect the schema (i.e., by add/deleting columns or tables).
     *
     * @return true if the query can affect the database's schema, false otherwise
     */
    public abstract boolean couldAffectSchema();

    /**
     * Executes the query against the database.
     *
     * @param globalState the global state containing the database connection.
     * @param fills optional parameters to fill placeholders in the query.
     * @return true if the query executed successfully, false otherwise.
     * @throws Exception if an error occurs during execution.
     */
    public abstract <G extends GlobalState<?, ?, C>> boolean execute(G globalState, String... fills) throws Exception;

    /**
     * Gets the set of expected errors for this query.
     *
     * @return the expected errors.
     */
    public abstract ExpectedErrors getExpectedErrors();

    @Override
    public String toString() {
        return getQueryString();
    }

    /**
     * Executes the query and returns the result set.
     *
     * @param globalState the global state containing the database connection.
     * @param fills optional parameters to fill placeholders in the query.
     * @return the result set.
     * @throws Exception if an error occurs during execution.
     */
    public <G extends GlobalState<?, ?, C>> SQLancerResultSet executeAndGet(G globalState, String... fills)
            throws Exception {
        throw new AssertionError();
    }

    /**
     * Executes the query and logs the query string.
     *
     * @param globalState the global state containing the database connection.
     * @return true if the query executed successfully, false otherwise.
     * @throws Exception if an error occurs during execution.
     */
    public <G extends GlobalState<?, ?, C>> boolean executeLogged(G globalState) throws Exception {
        logQueryString(globalState);
        return execute(globalState);
    }

    /**
     * Executes the query, logs the query string, and returns the result set.
     *
     * @param globalState the global state containing the database connection.
     * @return the result set.
     * @throws Exception if an error occurs during execution.
     */
    public <G extends GlobalState<?, ?, C>> SQLancerResultSet executeAndGetLogged(G globalState) throws Exception {
        logQueryString(globalState);
        return executeAndGet(globalState);
    }

    /**
     * Logs the query string if logging is enabled.
     *
     * @param globalState the global state containing the logger and options.
     */
    private <G extends GlobalState<?, ?, C>> void logQueryString(G globalState) {
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(getQueryString());
        }
    }

}
