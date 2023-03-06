package sqlancer.timescaledb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.SQLConnection;
import sqlancer.postgres.PostgresSchema;

public class TimeScaleDBSchema extends PostgresSchema {

    public TimeScaleDBSchema(List<TimeScaleDBTable> databaseTables, String databaseName) {
        super(new ArrayList<>(databaseTables), databaseName);
    }

    public static class TimeScaleDBTable extends PostgresTable {

        public TimeScaleDBTable(String tableName, List<PostgresColumn> columns, List<PostgresIndex> indexes, TableType tableType, List<PostgresStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, tableType, statistics, isView, isInsertable);
        }

        public TimeScaleDBTable(PostgresTable table) {
            super(table.getName(), table.getColumns(), table.getIndexes(), table.getTableType(), table.getStatistics(), table.isView(), table.isInsertable());
        }
    }

    public static TimeScaleDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        PostgresSchema schema = PostgresSchema.fromConnection(con, databaseName);
        List<TimeScaleDBTable> databaseTables = new ArrayList<>();
        try {
            try (Statement s = con.createStatement(); ResultSet rs = s.executeQuery("SELECT table_name FROM information_schema.tables")) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");

                    PostgresTable t = schema.getDatabaseTable(tableName);
                    if (t == null) {
                        continue;
                    }

                    TimeScaleDBTable tCitus = new TimeScaleDBTable(t);
                    databaseTables.add(tCitus);
                }
            }
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
        return new TimeScaleDBSchema(databaseTables, databaseName);
    }
}
