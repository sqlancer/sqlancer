package sqlancer.spark;

import sqlancer.SQLGlobalState;

public class SparkGlobalState extends SQLGlobalState<SparkOptions, SparkSchema> {

    @Override
    protected SparkSchema readSchema() throws Exception {
        return SparkSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
