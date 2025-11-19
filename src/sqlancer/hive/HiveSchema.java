package sqlancer.hive;

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
import sqlancer.hive.HiveSchema.HiveTable;

public class HiveSchema extends AbstractSchema<HiveGlobalState, HiveTable> {

    public enum HiveDataType {

        // TODO: support more types, e.g. TIMESTAMP, DATE, VARCHAR, CHAR, BINARY, ARRAY, MAP, STRUCT, UNIONTYPE...
        STRING, INT, DOUBLE, BOOLEAN;

        public static HiveDataType getRandomType() {
            return Randomly.fromList(Arrays.asList(values()));
        }
    }

    public static class HiveColumn extends AbstractTableColumn<HiveTable, HiveDataType> {

        public HiveColumn(String name, HiveTable table, HiveDataType type) {
            super(name, table, type);
        }
    }

    public static class HiveTables extends AbstractTables<HiveTable, HiveColumn> {

        public HiveTables(List<HiveTable> tables) {
            super(tables);
        }
    }

    public static class HiveTable extends AbstractRelationalTable<HiveColumn, TableIndex, HiveGlobalState> {

        public HiveTable(String name, List<HiveColumn> columns, boolean isView) {
            super(name, columns, Collections.emptyList(), isView);
        }
    }

    public HiveSchema(List<HiveTable> databaseTables) {
        super(databaseTables);
    }

    public static HiveSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<HiveTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<HiveColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            HiveTable t = new HiveTable(tableName, databaseColumns, isView);
            for (HiveColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);
        }
        return new HiveSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            ResultSet tableRs = s.executeQuery("SHOW TABLES");
            while (tableRs.next()) {
                String tableName = tableRs.getString(1);
                tableNames.add(tableName);
            }
        }
        return tableNames;
    }

    private static List<HiveColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<HiveColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("DESCRIBE %s", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("col_name");
                    String dataType = rs.getString("data_type");
                    HiveColumn c = new HiveColumn(columnName, null, getColumnType(dataType.toUpperCase()));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    private static HiveDataType getColumnType(String typeString) {
        return HiveDataType.valueOf(typeString.toUpperCase());
    }

    public HiveTables getRandomTableNonEmptyTables() {
        return new HiveTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }
}
