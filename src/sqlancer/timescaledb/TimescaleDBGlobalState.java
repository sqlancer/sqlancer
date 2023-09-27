package sqlancer.timescaledb;

import java.sql.SQLException;

import sqlancer.postgres.PostgresGlobalState;

public class TimescaleDBGlobalState extends PostgresGlobalState {
    @Override
    public TimescaleDBSchema readSchema() throws SQLException {
        return TimescaleDBSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
