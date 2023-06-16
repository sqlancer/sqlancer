package sqlancer.reducer.VirtualDB;

import sqlancer.SQLConnection;

import java.sql.Connection;
import java.sql.SQLException;

public class VirtualDBConnection extends SQLConnection {

    public VirtualDBConnection(Connection connection) {
        super(connection);
    }

    @Override
    public void close() throws SQLException {

    }
}
