
package sqlancer.mysql;

import java.sql.SQLException;

import sqlancer.GlobalState;

public class MySQLGlobalState extends GlobalState<MySQLOptions> {

    private MySQLSchema schema;

    public void setSchema(MySQLSchema schema) {
        this.schema = schema;
    }

    public MySQLSchema getSchema() {
        return schema;
    }

    @Override
    protected void updateSchema() throws SQLException {
        setSchema(MySQLSchema.fromConnection(getConnection(), getDatabaseName()));
    }

}
