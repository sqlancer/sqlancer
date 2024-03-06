
package sqlancer.oceanbase;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;

public class OceanBaseGlobalState extends SQLGlobalState<OceanBaseOptions, OceanBaseSchema> {

    @Override
    protected OceanBaseSchema readSchema() throws SQLException {
        return OceanBaseSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
