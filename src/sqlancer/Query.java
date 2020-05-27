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
     * @return true if the query was successful, false otherwise
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

}
