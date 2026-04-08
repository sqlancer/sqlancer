package sqlancer.spark;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.spark.SparkSchema.SparkTable;

public class SparkSchema extends AbstractSchema<SparkGlobalState, SparkTable> {

    public enum SparkDataType {
        STRING, INTEGER, DOUBLE, BOOLEAN, TIMESTAMP, DATE;

        public static SparkDataType getRandomType() {
            return Randomly.fromList(Arrays.asList(values()));
        }
    }

    public static class SparkColumn extends AbstractTableColumn<SparkTable, SparkDataType> {
        public SparkColumn(String name, SparkTable table, SparkDataType type) {
            super(name, table, type);
        }
    }

    public static class SparkTables extends AbstractTables<SparkTable, SparkColumn> {
        public SparkTables(List<SparkTable> tables) {
            super(tables);
        }
    }

    public static class SparkTable extends AbstractRelationalTable<SparkColumn, TableIndex, SparkGlobalState> {
        public SparkTable(String name, List<SparkColumn> columns, boolean isView) {
            super(name, columns, Collections.emptyList(), isView);
        }
    }

    public SparkSchema(List<SparkTable> databaseTables) {
        super(databaseTables);
    }

    public static SparkSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<SparkTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<SparkColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = matchesViewName(tableName);
            SparkTable t = new SparkTable(tableName, databaseColumns, isView);
            for (SparkColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);
        }
        return new SparkSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            ResultSet tableRs = s.executeQuery("SHOW TABLES");
            while (tableRs.next()) {
                // Spark SHOW TABLES output: database, tableName, isTemporary
                String tableName = tableRs.getString("tableName");
                tableNames.add(tableName);
            }
        }
        return tableNames;
    }

    private static List<SparkColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<SparkColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("DESCRIBE %s", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("col_name");
                    String dataType = rs.getString("data_type");
                    // Filter out Spark partition info or comments usually at bottom of describe
                    if (columnName.startsWith("#") || columnName.isEmpty()) {
                        continue;
                    }

                    columns.add(new SparkColumn(columnName, null, getColumnType(dataType)));
                }
            }
        }
        return columns;
    }

    private static SparkDataType getColumnType(String typeString) {
        String upper = typeString.toUpperCase();
        if (upper.startsWith("STRING") || upper.startsWith("VARCHAR") || upper.startsWith("CHAR")) {
            return SparkDataType.STRING;
        }
        if (upper.startsWith("INT") || upper.startsWith("BIGINT") || upper.startsWith("SMALLINT")) {
            return SparkDataType.INTEGER;
        }
        if (upper.startsWith("DOUBLE") || upper.startsWith("FLOAT") || upper.startsWith("DECIMAL")) {
            return SparkDataType.DOUBLE;
        }
        if (upper.startsWith("BOOLEAN")) {
            return SparkDataType.BOOLEAN;
        }
        if (upper.startsWith("TIMESTAMP")) {
            return SparkDataType.TIMESTAMP;
        }
        if (upper.startsWith("DATE")) {
            return SparkDataType.DATE;
        }
        return SparkDataType.STRING; // Fallback
    }

}
