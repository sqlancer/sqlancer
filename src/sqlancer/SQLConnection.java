package sqlancer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLConnection implements SQLancerDBConnection {

    private final Connection connection;
    private GlobalState<?, ?, ?> globalState;

    public SQLConnection(Connection connection) {
        this.connection = connection;
    }

    public void setGlobalState(GlobalState<?, ?, ?> globalState) {
        this.globalState = globalState;
    }

    public GlobalState<?, ?, ?> getGlobalState() {
        return globalState;
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        return meta.getDatabaseProductVersion();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    public Statement prepareStatement(String arg) throws SQLException {
        return connection.prepareStatement(arg);
    }

    public Statement createStatement() throws SQLException {
        return connection.createStatement();
    }
}
