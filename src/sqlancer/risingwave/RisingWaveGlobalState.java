package sqlancer.risingwave;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;

public class RisingWaveGlobalState extends SQLGlobalState<RisingWaveOptions, RisingWaveSchema> {
    @Override
    protected RisingWaveSchema readSchema() throws SQLException {
        return RisingWaveSchema.read(getConnection());
    }
}
