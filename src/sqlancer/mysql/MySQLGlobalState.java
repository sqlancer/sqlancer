
package sqlancer.mysql;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;
import sqlancer.mysql.MySQLOptions.MySQLOracleFactory;

public class MySQLGlobalState extends SQLGlobalState<MySQLOptions, MySQLSchema> {

    @Override
    protected MySQLSchema readSchema() throws SQLException {
        return MySQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOracleFactory.PQS);
    }

}
