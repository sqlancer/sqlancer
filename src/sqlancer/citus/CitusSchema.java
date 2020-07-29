package sqlancer.citus;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.postgres.PostgresSchema;

public class CitusSchema extends PostgresSchema {

    public CitusSchema(List<CitusTable> databaseTables, String databaseName) {
        // FIXME: Will casting to PostgresTable lose CitusTable features?
        super(new ArrayList<>(databaseTables), databaseName);
    }

    public static class CitusTable extends PostgresTable {

        private PostgresColumn distributionColumn = null;
        private Integer colocationId = null;

        public CitusTable(String tableName, List<PostgresColumn> columns, List<PostgresIndex> indexes,
                TableType tableType, List<PostgresStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, tableType, statistics, isView, isInsertable);
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

    public static CitusSchema fromConnection(Connection con, String databaseName) throws SQLException {
        Exception ex = null;
        try {
            List<CitusTable> databaseTables = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT table_name, table_schema, table_type, is_insertable_into, column_to_column_name(logicalrelid, partkey) AS dist_col_name, colocationid FROM information_schema.tables LEFT OUTER JOIN pg_dist_partition ON logicalrelid=table_name::regclass WHERE table_schema='public' OR table_schema LIKE 'pg_temp_%';")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String tableTypeSchema = rs.getString("table_schema");
                        boolean isInsertable = rs.getBoolean("is_insertable_into");
                        String distributionColumnName = rs.getString("dist_col_name");
                        Integer colocationId = rs.getInt("colocationid");
                        if (rs.wasNull()) {
                            colocationId = null;
                        }
                        // TODO: also check insertable
                        // TODO: insert into view?
                        // FIXME: This part looks like there will be improvements, should we be concerned that I am overwriting the method?
                        boolean isView = tableName.startsWith("v"); // tableTypeStr.contains("VIEW") ||
                                                                    // tableTypeStr.contains("LOCAL TEMPORARY") &&
                                                                    // !isInsertable;
                        PostgresTable.TableType tableType = getTableType(tableTypeSchema);
                        List<PostgresColumn> databaseColumns = getTableColumns(con, tableName);
                        List<PostgresIndex> indexes = getIndexes(con, tableName);
                        List<PostgresStatisticsObject> statistics = getStatistics(con);
                        CitusTable t = new CitusTable(tableName, databaseColumns, indexes, tableType, statistics,
                                isView, isInsertable);
                        if (distributionColumnName != null && !distributionColumnName.equals("")) {
                            PostgresColumn distributionColumn = databaseColumns.stream().filter(c -> c.getName().equals(distributionColumnName)).collect(Collectors.toList()).get(0);
                            t.setDistributionColumn(distributionColumn);
                        }
                        if (colocationId != null) {
                            t.setColocationId(colocationId);
                        }
                        for (PostgresColumn c : databaseColumns) {
                            c.setTable(t);
                        }
                        databaseTables.add(t);
                    }
                }
            }
            return new CitusSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            ex = e;
        }
        throw new AssertionError(ex);
    }
    
}
