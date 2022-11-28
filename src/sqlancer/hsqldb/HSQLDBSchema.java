package sqlancer.hsqldb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;

public class HSQLDBSchema extends AbstractSchema<HSQLDBProvider.HSQLDBGlobalState, HSQLDBSchema.HSQLDBTable> {

    public HSQLDBSchema(List<HSQLDBTable> databaseTables) {
        super(databaseTables);
    }

    public static HSQLDBSchema fromConnection(SQLConnection connection, String databaseName) throws SQLException {
        List<HSQLDBSchema.HSQLDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(connection);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<HSQLDBSchema.HSQLDBColumn> databaseColumns = getTableColumns(connection, tableName);
            boolean isView = tableName.startsWith("v");
            HSQLDBSchema.HSQLDBTable t = new HSQLDBSchema.HSQLDBTable(tableName, databaseColumns, isView);
            for (HSQLDBSchema.HSQLDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new HSQLDBSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC';")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("TABLE_NAME"));
                }
            }
        }
        return tableNames;
    }

    private static List<HSQLDBColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<HSQLDBColumn> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            String sql = "SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME = '%s';";
            try (ResultSet rs = s.executeQuery(String.format(sql, tableName))) {
                while (rs.next()) {
                    HSQLDBDataType dataType = HSQLDBDataType.from(rs.getString("TYPE_NAME"));
                    HSQLDBCompositeDataType compositeDataType = new HSQLDBCompositeDataType(dataType,
                            rs.getInt("COLUMN_SIZE"));
                    HSQLDBColumn column = new HSQLDBColumn(rs.getString("COLUMN_NAME"), null, compositeDataType);
                    tableNames.add(column);
                }
            }
        }
        return tableNames;
    }

    public static class HSQLDBTable
            extends AbstractRelationalTable<HSQLDBSchema.HSQLDBColumn, TableIndex, HSQLDBProvider.HSQLDBGlobalState> {

        public HSQLDBTable(String tableName, List<HSQLDBSchema.HSQLDBColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static class HSQLDBColumn
            extends AbstractTableColumn<HSQLDBSchema.HSQLDBTable, HSQLDBSchema.HSQLDBCompositeDataType> {

        public HSQLDBColumn(String name, HSQLDBTable table, HSQLDBCompositeDataType type) {
            super(name, table, type);
        }
    }

    public enum HSQLDBDataType {

        INTEGER, DOUBLE, BOOLEAN, CHAR, VARCHAR, BINARY, TIME, DATE, TIMESTAMP, NULL;

        public static HSQLDBSchema.HSQLDBDataType getRandomWithoutNull() {
            HSQLDBSchema.HSQLDBDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == HSQLDBSchema.HSQLDBDataType.NULL);
            return dt;
        }

        public static HSQLDBDataType from(String typeName) {
            for (HSQLDBDataType value : HSQLDBDataType.values()) {
                if (value.name().equals(typeName)) {
                    return value;
                }
            }
            return NULL;
        }
    }

    public static class HSQLDBCompositeDataType {
        private final int size;
        private final HSQLDBDataType type;

        public HSQLDBCompositeDataType(HSQLDBDataType type, int size) {
            this.type = type;
            this.size = size;
        }

        public static HSQLDBCompositeDataType getRandomWithoutNull() {
            HSQLDBSchema.HSQLDBDataType type = HSQLDBSchema.HSQLDBDataType.getRandomWithoutNull();
            return getRandomWithType(type);
        }

        public static HSQLDBCompositeDataType getRandomWithType(HSQLDBSchema.HSQLDBDataType type) {
            int size;
            switch (type) {
            case VARCHAR:
            case CHAR:
            case TIME:
            case BINARY:
            case TIMESTAMP:
                size = Randomly.fromOptions(4, 6, 8);
                break;
            case BOOLEAN:
            case INTEGER:
            case DOUBLE:
                // case UUID:
                // case OTHER:
            case DATE:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new HSQLDBSchema.HSQLDBCompositeDataType(type, size);
        }

        public HSQLDBDataType getType() {
            return type;
        }

        public int getSize() {
            return size;
        }
    }
}
