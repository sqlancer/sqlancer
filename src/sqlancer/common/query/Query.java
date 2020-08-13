package sqlancer.common.query;

import java.sql.SQLException;

import sqlancer.GlobalState;

public abstract class Query {

    public abstract String getQueryString();

    /**
     * Whether the query could affect the schema (i.e., by add/deleting columns or tables).
     *
     * @return true if the query can affect the database's schema, false otherwise
     */
    public abstract boolean couldAffectSchema();

    public abstract boolean execute(GlobalState<?, ?> globalState, String... fills) throws SQLException;

    public abstract ExpectedErrors getExpectedErrors();

    @Override
    public String toString() {
        return getQueryString();
    }

    public SQLancerResultSet executeAndGet(GlobalState<?, ?> globalState, String... fills) throws SQLException {
        throw new AssertionError();
    }

    public boolean executeLogged(GlobalState<?, ?> globalState) throws SQLException {
        logQueryString(globalState);
        return execute(globalState);
    }

    public SQLancerResultSet executeAndGetLogged(GlobalState<?, ?> globalState) throws SQLException {
        logQueryString(globalState);
        return executeAndGet(globalState);
    }

    private void logQueryString(GlobalState<?, ?> globalState) {
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(getQueryString());
        }
    }

}
