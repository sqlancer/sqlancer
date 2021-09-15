package sqlancer.citus;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.SQLConnection;
import sqlancer.postgres.PostgresSchema;

public class CitusSchema extends PostgresSchema {

    public CitusSchema(List<CitusTable> databaseTables, String databaseName) {
        super(new ArrayList<>(databaseTables), databaseName);
    }

    public static class CitusTable extends PostgresTable {

        private PostgresColumn distributionColumn;
        // colocationId is null for local tables
        private Integer colocationId;

        public CitusTable(String tableName, List<PostgresColumn> columns, List<PostgresIndex> indexes,
                TableType tableType, List<PostgresStatisticsObject> statistics, boolean isView, boolean isInsertable,
                PostgresColumn distributionColumn, Integer colocationId) {
            super(tableName, columns, indexes, tableType, statistics, isView, isInsertable);
            this.distributionColumn = distributionColumn;
            this.colocationId = colocationId;
        }

        public CitusTable(PostgresTable table, PostgresColumn distributionColumn, Integer colocationId) {
            super(table.getName(), table.getColumns(), table.getIndexes(), table.getTableType(), table.getStatistics(),
                    table.isView(), table.isInsertable());
            this.distributionColumn = distributionColumn;
            this.colocationId = colocationId;
        }

        public void setDistributionColumn(PostgresColumn distributionColumn) {
            this.distributionColumn = distributionColumn;
        }

        public void setColocationId(Integer colocationId) {
            this.colocationId = colocationId;
        }

        public PostgresColumn getDistributionColumn() {
            return this.distributionColumn;
        }

        public Integer getColocationId() {
            return this.colocationId;
        }

    }

    public static CitusSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        PostgresSchema schema = PostgresSchema.fromConnection(con, databaseName);
        try {
            List<CitusTable> databaseTables = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT table_name, column_to_column_name(logicalrelid, partkey) AS dist_col_name, colocationid FROM information_schema.tables LEFT OUTER JOIN pg_dist_partition ON logicalrelid=table_name::regclass WHERE table_schema='public' OR table_schema LIKE 'pg_temp_%';")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        /* citus_tables is a helper view, we don't need to test with it so we let's ignore it */
                        if (tableName.equals("citus_tables")) {
                            continue;
                        }
                        String distributionColumnName = rs.getString("dist_col_name");
                        Integer colocationId = rs.getInt("colocationid");
                        if (rs.wasNull()) {
                            colocationId = null;
                        }
                        PostgresTable t = schema.getDatabaseTable(tableName);
                        PostgresColumn distributionColumn = null;
                        if (t == null) {
                            continue;
                        }
                        if (distributionColumnName != null && !distributionColumnName.equals("")) {
                            distributionColumn = t.getColumns().stream()
                                    .filter(c -> c.getName().equals(distributionColumnName))
                                    .collect(Collectors.toList()).get(0);
                        }
                        CitusTable tCitus = new CitusTable(t, distributionColumn, colocationId);
                        databaseTables.add(tCitus);
                    }
                }
            }
            return new CitusSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
    }

}
