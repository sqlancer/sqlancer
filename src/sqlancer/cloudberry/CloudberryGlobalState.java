package sqlancer.cloudberry;

import java.sql.SQLException;

import sqlancer.SQLConnection;
import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

public class CloudberryGlobalState extends PostgresGlobalState {

    @Override
    public CloudberrySchema readSchema() throws SQLException {
        return CloudberrySchema.fromConnection(getConnection(), getDatabaseName());
    }

    /**
     * Adds Cloudberry's expected errors to every statement executed during database generation, so that the inherited
     * PostgreSQL generators do not each need a Cloudberry subclass. Oracles add these errors themselves.
     */
    @Override
    public boolean executeStatement(Query<SQLConnection> q, String... fills) throws Exception {
        if (q instanceof SQLQueryAdapter) {
            ExpectedErrors errors = ((SQLQueryAdapter) q).getExpectedErrors();
            if (errors != null) {
                CloudberryCommon.addCloudberryErrors(errors);
            }
        }
        return super.executeStatement(q, fills);
    }

}
