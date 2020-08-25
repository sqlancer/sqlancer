
package sqlancer.mysql;

import java.sql.SQLException;

import sqlancer.GlobalState;
import sqlancer.mysql.MySQLOptions.MySQLOracleFactory;

public class MySQLGlobalState extends GlobalState<MySQLOptions, MySQLSchema> {

    @Override
    protected void updateSchema() throws SQLException {
        setSchema(MySQLSchema.fromConnection(getConnection(), getDatabaseName()));
    }

    public boolean usesPQS() {
        return getDmbsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOracleFactory.PQS);
    }

}
