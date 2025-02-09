package sqlancer.postgres;

import java.sql.SQLException;

import sqlancer.SQLConnection;
import sqlancer.common.BaseGlobalState;

public class PostgresGlobalState extends BaseGlobalState<PostgresOptions, PostgresSchema> {

    @Override
    public PostgresSchema readSchema() throws SQLException {
        return PostgresSchema.fromConnection(getConnection(), getDatabaseName());
    }
}