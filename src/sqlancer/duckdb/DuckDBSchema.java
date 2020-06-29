package sqlancer.duckdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.schema.AbstractSchema;
import sqlancer.schema.AbstractTable;
import sqlancer.schema.AbstractTableColumn;
import sqlancer.schema.AbstractTables;
import sqlancer.schema.TableIndex;

public class DuckDBSchema extends AbstractSchema<DuckDBTable> {

    public enum DuckDBDataType {

        INT, VARCHAR, BOOLEAN, FLOAT, DATE, TIMESTAMP;

        public static DuckDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static class DuckDBCompositeDataType {

        private final DuckDBDataType dataType;

        private final int size;

        public DuckDBCompositeDataType(DuckDBDataType dataType) {
            this.dataType = dataType;
            this.size = -1;
        }

        public DuckDBCompositeDataType(DuckDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public DuckDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static DuckDBCompositeDataType getRandom() {
            DuckDBDataType type = DuckDBDataType.getRandom();
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 8);
                break;
            case BOOLEAN:
            case VARCHAR:
            case DATE:
            case TIMESTAMP:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new DuckDBCompositeDataType(type, size);
        }

        public static DuckDBCompositeDataType getInt(int size) {
            return new DuckDBCompositeDataType(DuckDBDataType.INT, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("BIGINT", "INT8");
                case 4:
                    return Randomly.fromOptions("INTEGER", "INT", "INT4", "SIGNED");
                case 2:
                    return Randomly.fromOptions("SMALLINT", "INT2");
                case 1:
                    return Randomly.fromOptions("TINYINT", "INT1");
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return "VARCHAR";
            case FLOAT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("DOUBLE", "NUMERIC");
                case 4:
                    return Randomly.fromOptions("REAL", "FLOAT4");
                default:
                    throw new AssertionError(size);
                }
            case BOOLEAN:
                return Randomly.fromOptions("BOOLEAN", "BOOL");
            case TIMESTAMP:
                return Randomly.fromOptions("TIMESTAMP", "DATETIME");
            case DATE:
                return Randomly.fromOptions("DATE");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class DuckDBColumn extends AbstractTableColumn<DuckDBTable, DuckDBCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public DuckDBColumn(String name, DuckDBCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class DuckDBTables extends AbstractTables<DuckDBTable, DuckDBColumn> {

        public DuckDBTables(List<DuckDBTable> tables) {
            super(tables);
        }

    }

    public DuckDBSchema(List<DuckDBTable> databaseTables) {
        super(databaseTables);
    }

    public DuckDBTables getRandomTableNonEmptyTables() {
        return new DuckDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static DuckDBCompositeDataType getColumnType(String typeString) {
        DuckDBDataType primitiveType;
        int size = -1;
        switch (typeString) {
        case "INTEGER":
            primitiveType = DuckDBDataType.INT;
            size = 4;
            break;
        case "SMALLINT":
            primitiveType = DuckDBDataType.INT;
            size = 2;
            break;
        case "BIGINT":
            primitiveType = DuckDBDataType.INT;
            size = 8;
            break;
        case "TINYINT":
            primitiveType = DuckDBDataType.INT;
            size = 1;
            break;
        case "VARCHAR":
            primitiveType = DuckDBDataType.VARCHAR;
            break;
        case "FLOAT":
            primitiveType = DuckDBDataType.FLOAT;
            size = 4;
            break;
        case "DOUBLE":
            primitiveType = DuckDBDataType.FLOAT;
            size = 8;
            break;
        case "BOOLEAN":
            primitiveType = DuckDBDataType.BOOLEAN;
            break;
        case "DATE":
            primitiveType = DuckDBDataType.DATE;
            break;
        case "TIMESTAMP":
            primitiveType = DuckDBDataType.TIMESTAMP;
            break;
        default:
            throw new AssertionError(typeString);
        }
        return new DuckDBCompositeDataType(primitiveType, size);
    }

    public static class DuckDBTable extends AbstractTable<DuckDBColumn, TableIndex> {

        public DuckDBTable(String tableName, List<DuckDBColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static DuckDBSchema fromConnection(Connection con, String databaseName) throws SQLException {
        List<DuckDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<DuckDBColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            DuckDBTable t = new DuckDBTable(tableName, databaseColumns, isView);
            for (DuckDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new DuckDBSchema(databaseTables);
    }

    private static List<String> getTableNames(Connection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM sqlite_master()")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("name"));
                }
            }
        }
        return tableNames;
    }

    private static List<DuckDBColumn> getTableColumns(Connection con, String tableName) throws SQLException {
        List<DuckDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SELECT * FROM pragma_table_info('%s');", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String dataType = rs.getString("type");
                    boolean isNullable = rs.getString("notnull").contentEquals("false");
                    boolean isPrimaryKey = rs.getString("pk").contains("true");
                    DuckDBColumn c = new DuckDBColumn(columnName, getColumnType(dataType), isPrimaryKey, isNullable);
                    columns.add(c);
                }
            }
        }
        if (columns.stream().noneMatch(c -> c.isPrimaryKey())) {
            // https://github.com/cwida/duckdb/issues/589
            // https://github.com/cwida/duckdb/issues/588
            // TODO: implement an option to enable/disable rowids
            columns.add(new DuckDBColumn("rowid", new DuckDBCompositeDataType(DuckDBDataType.INT, 4), false, false));
        }
        return columns;
    }

}
