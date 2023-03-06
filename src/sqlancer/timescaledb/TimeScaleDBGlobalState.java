package sqlancer.timescaledb;

import sqlancer.postgres.PostgresGlobalState;

import java.sql.SQLException;

public class TimeScaleDBGlobalState extends PostgresGlobalState {
    @Override
    public TimeScaleDBSchema readSchema() throws SQLException {
        return TimeScaleDBSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
