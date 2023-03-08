package sqlancer.timescaledb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.SQLConnection;
import sqlancer.postgres.PostgresSchema;

public class TimescaleDBSchema extends PostgresSchema {

    public TimescaleDBSchema(List<TimescaleDBTable> databaseTables, String databaseName) {
        super(new ArrayList<>(databaseTables), databaseName);
    }

    public static class TimescaleDBTable extends PostgresTable {
        public TimescaleDBTable(String tableName, List<PostgresColumn> columns, List<PostgresIndex> indexes,
                TableType tableType, List<PostgresStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, tableType, statistics, isView, isInsertable);
        }

        public TimescaleDBTable(PostgresTable table) {
            super(table.getName(), table.getColumns(), table.getIndexes(), table.getTableType(), table.getStatistics(),
                    table.isView(), table.isInsertable());
        }
    }

    public static TimescaleDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        PostgresSchema schema = PostgresSchema.fromConnection(con, databaseName);
        List<TimescaleDBTable> databaseTables = new ArrayList<>();
        try (Statement s = con.createStatement();
                ResultSet rs = s.executeQuery("SELECT table_name FROM information_schema.tables")) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");

                PostgresTable t = schema.getDatabaseTable(tableName);
                if (t == null) {
                    continue;
                }

                TimescaleDBTable table = new TimescaleDBTable(t);
                databaseTables.add(table);
            }
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
        return new TimescaleDBSchema(databaseTables, databaseName);
    }
}
