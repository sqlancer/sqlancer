
package sqlancer.mysql;

import java.sql.SQLException;

import sqlancer.GlobalState;

public class MySQLGlobalState extends GlobalState<MySQLOptions, MySQLSchema> {

    @Override
    protected void updateSchema() throws SQLException {
        setSchema(MySQLSchema.fromConnection(getConnection(), getDatabaseName()));
    }

}
