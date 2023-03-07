package sqlancer.timescaledb;

import java.sql.SQLException;

import sqlancer.postgres.PostgresGlobalState;

public class TimeScaleDBGlobalState extends PostgresGlobalState {
    @Override
    public TimeScaleDBSchema readSchema() throws SQLException {
        return TimeScaleDBSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
