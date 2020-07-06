package sqlancer;

import java.sql.Connection;
import java.sql.ResultSet;
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
    public abstract boolean execute(Connection con) throws SQLException;

    public abstract Collection<String> getExpectedErrors();

    @Override
    public String toString() {
        return getQueryString();
    }

    public ResultSet executeAndGet(Connection con) throws SQLException {
        throw new AssertionError();
    }

    public boolean executeLogged(GlobalState<?> globalState) throws SQLException {
        logQueryString(globalState);
        return execute(globalState.getConnection());
    }

    public ResultSet executeAndGetLogged(GlobalState<?> globalState) throws SQLException {
        logQueryString(globalState);
        return executeAndGet(globalState.getConnection());
    }

    private void logQueryString(GlobalState<?> globalState) {
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(getQueryString());
        }
    }

}
