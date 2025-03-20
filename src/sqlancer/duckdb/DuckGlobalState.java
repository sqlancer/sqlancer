package sqlancer.duckdb;

import sqlancer.SQLGlobalState;

public class DuckGlobalState extends SQLGlobalState<DuckDBOptions, DuckDBSchema> {
    @Override
    protected DuckDBSchema readSchema() throws Exception {
        return DuckDBSchema.fromConnection(getConnection(), getDatabaseName());
    }
}
