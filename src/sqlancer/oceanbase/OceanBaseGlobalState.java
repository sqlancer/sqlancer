
package sqlancer.oceanbase;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;
import sqlancer.oceanbase.OceanBaseOptions.OceanBaseOracleFactory;

public class OceanBaseGlobalState extends SQLGlobalState<OceanBaseOptions, OceanBaseSchema> {

    @Override
    protected OceanBaseSchema readSchema() throws SQLException {
        return OceanBaseSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == OceanBaseOracleFactory.PQS);
    }

}
