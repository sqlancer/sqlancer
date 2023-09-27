package sqlancer.tidb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public class TiDBSchema extends AbstractSchema<TiDBGlobalState, TiDBTable> {

    public enum TiDBDataType {

        INT, TEXT, BOOL, FLOATING, CHAR, DECIMAL, NUMERIC, BLOB;

        private final boolean isPrimitive;

        TiDBDataType() {
            isPrimitive = true;
        }

        TiDBDataType(boolean isPrimitive) {
            this.isPrimitive = isPrimitive;
        }

        public static TiDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public boolean isPrimitive() {
            return isPrimitive;
        }

        public boolean isNumeric() {
            switch (this) {
            case INT:
            case DECIMAL:
            case FLOATING:
            case BOOL:
            case NUMERIC:
                return true;
            case CHAR:
            case TEXT:
            case BLOB:
                return false;
            default:
                throw new AssertionError(this);
            }
        }

        public boolean canHaveDefault() {
            switch (this) {
            case INT:
            case DECIMAL:
            case FLOATING:
            case BOOL:
            case CHAR:
                return true;
            case NUMERIC:
            case TEXT:
            case BLOB:
                return false;
            default:
                throw new AssertionError(this);
            }
        }
    }

    public static class TiDBCompositeDataType {

        private final TiDBDataType dataType;

        private final int size;

        public TiDBCompositeDataType(TiDBDataType dataType) {
            this.dataType = dataType;
            this.size = -1;
        }

        public TiDBCompositeDataType(TiDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public TiDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static TiDBCompositeDataType getInt(int size) {
            return new TiDBCompositeDataType(TiDBDataType.INT, size);
        }

        public static TiDBCompositeDataType getRandom() {
            TiDBDataType primitiveType = TiDBDataType.getRandom();
            int size = -1;
            switch (primitiveType) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOATING:
                size = Randomly.fromOptions(4, 8);
                break;
            default:
                break;
            }
            return new TiDBCompositeDataType(primitiveType, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 1:
                    return "TINYINT";
                case 2:
                    return "SMALLINT";
                case 3:
                    return "MEDIUMINT";
                case 4:
                    return "INTEGER";
                case 8:
                    return "BIGINT";
                default:
                    throw new AssertionError(size);
                }
            case FLOATING:
                switch (size) {
                case 4:
                    return "FLOAT";
                case 8:
                    return "DOUBLE";
                default:
                    throw new AssertionError(size);
                }
            default:
                return getPrimitiveDataType().toString();
            }
        }

    }

    public static class TiDBColumn extends AbstractTableColumn<TiDBTable, TiDBCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;
        private final boolean hasDefault;

        public TiDBColumn(String name, TiDBCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable,
                boolean hasDefault) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
            this.hasDefault = hasDefault;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public boolean hasDefault() {
            return hasDefault;
        }

    }

    public static class TiDBTables extends AbstractTables<TiDBTable, TiDBColumn> {

        public TiDBTables(List<TiDBTable> tables) {
            super(tables);
        }

    }

    public TiDBSchema(List<TiDBTable> databaseTables) {
        super(databaseTables);
    }

    public TiDBTables getRandomTableNonEmptyTables() {
        return new TiDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public int getIndexCount() {
        int count = 0;
        for (TiDBTable table : getDatabaseTables()) {
            count += table.getIndexes().size();
        }
        return count;
    }

    private static TiDBCompositeDataType getColumnType(String typeString) {
        String trimmedStringType = typeString.replace(" zerofill", "").replace(" unsigned", "");
        if (trimmedStringType.contains("decimal")) {
            return new TiDBCompositeDataType(TiDBDataType.DECIMAL);
        }
        if (trimmedStringType.startsWith("var_string") || trimmedStringType.contains("binary")
                || trimmedStringType.startsWith("varchar")) {
            return new TiDBCompositeDataType(TiDBDataType.TEXT);
        }
        if (trimmedStringType.startsWith("char")) {
            return new TiDBCompositeDataType(TiDBDataType.CHAR);
        }
        TiDBDataType primitiveType;
        int size = -1;
        if (trimmedStringType.startsWith("bigint")) {
            primitiveType = TiDBDataType.INT;
            size = 8;
        } else {
            switch (trimmedStringType) {
            case "text":
            case "mediumtext":
            case "longtext":
            case "tinytext":
                primitiveType = TiDBDataType.TEXT;
                break;
            case "float":
                size = 4;
                primitiveType = TiDBDataType.FLOATING;
                break;
            case "double":
            case "double(8,6)": // workaround to address https://github.com/sqlancer/sqlancer/issues/669
            case "double(23,16)":
                size = 8;
                primitiveType = TiDBDataType.FLOATING;
                break;
            case "tinyint(1)":
                primitiveType = TiDBDataType.BOOL;
                size = 1;
                break;
            case "null":
                primitiveType = TiDBDataType.INT;
                size = 1;
                break;
            case "tinyint(3)":
            case "tinyint(4)":
                primitiveType = TiDBDataType.INT;
                size = 1;
                break;
            case "smallint(5)":
            case "smallint(6)":
                primitiveType = TiDBDataType.INT;
                size = 2;
                break;
            case "int(10)":
            case "int(11)":
                primitiveType = TiDBDataType.INT;
                size = 4;
                break;
            case "blob":
            case "longblob":
            case "tinyblob":
                primitiveType = TiDBDataType.BLOB;
                break;
            case "date":
            case "datetime":
            case "datetime(6)": // workaround to address https://github.com/sqlancer/sqlancer/issues/669
            case "timestamp":
            case "time":
            case "year":
                primitiveType = TiDBDataType.NUMERIC;
                break;
            default:
                throw new AssertionError(trimmedStringType);
            }
        }
        return new TiDBCompositeDataType(primitiveType, size);
    }

    public static class TiDBTable extends AbstractRelationalTable<TiDBColumn, TableIndex, TiDBGlobalState> {

        public TiDBTable(String tableName, List<TiDBColumn> columns, List<TableIndex> indexes, boolean isView) {
            super(tableName, columns, indexes, isView);
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static TiDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<TiDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<TiDBColumn> databaseColumns = getTableColumns(con, tableName);
            // Ignore invalid views
            if (databaseColumns.isEmpty()) {
                continue;
            }
            List<TableIndex> indexes = getIndexes(con, tableName);
            boolean isView = tableName.startsWith("v");
            TiDBTable t = new TiDBTable(tableName, databaseColumns, indexes, isView);
            for (TiDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new TiDBSchema(databaseTables);
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

    private static List<TableIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<TableIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SHOW INDEX FROM %s", tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("Key_name");
                    indexes.add(TableIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<TiDBColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<TiDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SHOW COLUMNS FROM " + tableName)) {
                while (rs.next()) {
                    String columnName = rs.getString("Field");
                    String dataType = rs.getString("Type");
                    boolean isNullable = rs.getString("Null").contentEquals("YES");
                    boolean isPrimaryKey = rs.getString("Key").contains("PRI");
                    boolean hasDefault = rs.getString("Default") != null;
                    TiDBColumn c = new TiDBColumn(columnName, getColumnType(dataType), isPrimaryKey, isNullable,
                            hasDefault);
                    columns.add(c);
                }
            }
        } catch (SQLException e) { // Happens when
        }
        return columns;
    }

}
