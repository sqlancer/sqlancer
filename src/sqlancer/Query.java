package sqlancer;

import java.sql.SQLException;
import java.util.Collection;

public abstract class Query {

    public abstract String getQueryString();

    /**
     * Whether the query could affect the schema (i.e., by add/deleting columns or tables).
     *
     * @return
     */
    public abstract boolean couldAffectSchema();

    /**
     *
     * @param con
     *
     * @return true if the query was successful, false otherwise
     *
     * @throws SQLException
     */
    public abstract boolean execute(GlobalState<?, ?> globalState, String... fills) throws SQLException;

    public abstract Collection<String> getExpectedErrors();

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
