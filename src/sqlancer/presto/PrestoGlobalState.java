package sqlancer.presto;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;

public class PrestoGlobalState extends SQLGlobalState<PrestoOptions, PrestoSchema> {

    @Override
    protected PrestoSchema readSchema() throws SQLException {
        return PrestoSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
