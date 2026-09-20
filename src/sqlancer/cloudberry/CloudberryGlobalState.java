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
     * Append Cloudberry-specific expected errors to every statement executed during database generation. The reused
     * PostgreSQL statement generators (INSERT, ALTER TABLE, CREATE INDEX, REINDEX, ...) carry only PostgreSQL's
     * expected errors, so Cloudberry's MPP restrictions (append-optimized limitations, distribution-key rules, ...)
     * would otherwise surface as spurious findings. Doing it here covers all inherited generators without subclassing
     * each one. Oracle queries are unaffected — they attach Cloudberry errors themselves.
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
