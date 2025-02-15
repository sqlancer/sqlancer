package sqlancer.postgres;

import java.sql.SQLException;

import sqlancer.ExpandedGlobalState;

public class PostgresGlobalState extends ExpandedGlobalState<PostgresOptions, PostgresSchema> {

    @Override
    public PostgresSchema readSchema() throws SQLException {
        return PostgresSchema.fromConnection(getConnection(), getDatabaseName());
    }

}
