package sqlancer.cloudberry;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sqlancer.SQLConnection;
import sqlancer.postgres.PostgresSchema;

/**
 * A {@link PostgresSchema} that additionally records, per table, the Cloudberry distribution policy type ({@code 'p'} =
 * hash-distributed, {@code 'r'} = replicated, {@code null} = randomly distributed / no policy) and the table access
 * method ({@code heap}, {@code ao_row}, {@code ao_column}, {@code pax}). The metadata is informational — the reused
 * PostgreSQL oracles do not depend on it — so schema reading falls back gracefully to plain PostgreSQL tables if the
 * Cloudberry catalogs are unavailable.
 */
public class CloudberrySchema extends PostgresSchema {

    public CloudberrySchema(List<CloudberryTable> databaseTables, String databaseName) {
        super(new ArrayList<>(databaseTables), databaseName);
    }

    public static class CloudberryTable extends PostgresTable {

        private final String accessMethod;
        private final Character policyType;

        public CloudberryTable(PostgresTable table, String accessMethod, Character policyType) {
            super(table.getName(), table.getColumns(), table.getIndexes(), table.getTableType(), table.getStatistics(),
                    table.isView(), table.isInsertable());
            this.accessMethod = accessMethod;
            this.policyType = policyType;
        }

        public String getAccessMethod() {
            return accessMethod;
        }

        public Character getPolicyType() {
            return policyType;
        }

        public boolean isReplicated() {
            return policyType != null && policyType == 'r';
        }

        public boolean isHashDistributed() {
            return policyType != null && policyType == 'p';
        }

    }

    private static final class TableMeta {
        private final String accessMethod;
        private final Character policyType;

        TableMeta(String accessMethod, Character policyType) {
            this.accessMethod = accessMethod;
            this.policyType = policyType;
        }
    }

    public static CloudberrySchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        PostgresSchema schema = PostgresSchema.fromConnection(con, databaseName);
        Map<String, TableMeta> meta = readCloudberryMeta(con);
        List<CloudberryTable> databaseTables = new ArrayList<>();
        for (PostgresTable t : schema.getDatabaseTables()) {
            TableMeta m = meta.get(t.getName());
            String accessMethod = m == null ? null : m.accessMethod;
            Character policyType = m == null ? null : m.policyType;
            databaseTables.add(new CloudberryTable(t, accessMethod, policyType));
        }
        return new CloudberrySchema(databaseTables, databaseName);
    }

    private static Map<String, TableMeta> readCloudberryMeta(SQLConnection con) {
        Map<String, TableMeta> meta = new HashMap<>();
        String query = "SELECT c.relname AS table_name, am.amname AS access_method, p.policytype AS policytype "
                + "FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid "
                + "LEFT JOIN pg_am am ON c.relam = am.oid "
                + "LEFT JOIN gp_distribution_policy p ON p.localoid = c.oid "
                + "WHERE c.relkind = 'r' AND (n.nspname = 'public' OR n.nspname LIKE 'pg_temp%');";
        try (Statement s = con.createStatement(); ResultSet rs = s.executeQuery(query)) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String accessMethod = rs.getString("access_method");
                String policy = rs.getString("policytype");
                Character policyType = policy == null || policy.isEmpty() ? null : policy.charAt(0);
                meta.put(tableName, new TableMeta(accessMethod, policyType));
            }
        } catch (SQLException e) {
            // Cloudberry catalogs unavailable (e.g. running against vanilla PostgreSQL): metadata stays empty.
            return new HashMap<>();
        }
        return meta;
    }
}
