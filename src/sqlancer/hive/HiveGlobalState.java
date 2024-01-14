package sqlancer.hive;

import sqlancer.SQLGlobalState;

public class HiveGlobalState extends SQLGlobalState<HiveOptions, HiveSchema> {

    @Override
    protected HiveSchema readSchema() throws Exception {
        return HiveSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
