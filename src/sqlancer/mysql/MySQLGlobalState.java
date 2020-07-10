
package sqlancer.mysql;

import java.sql.SQLException;

import sqlancer.GlobalState;

public class MySQLGlobalState extends GlobalState<MySQLOptions> {

    private MySQLSchema schema;

    public MySQLSchema getSchema() {
        if (schema == null) {
            try {
                updateSchema();
            } catch (SQLException e) {
                throw new AssertionError();
            }
        }
        return schema;
    }

    @Override
    protected void updateSchema() throws SQLException {
        this.schema = MySQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

}
