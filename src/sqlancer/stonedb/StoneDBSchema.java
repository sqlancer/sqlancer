package sqlancer.stonedb;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;
import sqlancer.duckdb.DuckDBProvider;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.stonedb.StoneDBProvider;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StoneDBSchema extends AbstractSchema<StoneDBProvider.StoneDBGlobalState, StoneDBSchema.StoneDBTable> {

    public enum StoneDBDataType {

        TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, YEAR, TIME, DATE, DATETIME, TIMESTAMP, CHAR,
        VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB;

        public static StoneDBDataType getRandomWithoutNull() {
            return Randomly.fromOptions(values());
        }

    }

    public static StoneDBCompositeDataType getRandomWithoutNull() {
        StoneDBDataType type = StoneDBDataType.getRandomWithoutNull();
        int size = -1;
        switch (type) {
        case TINYINT:
            size = 1;
            break;
        case SMALLINT:
            size = 2;
            break;
        case MEDIUMINT:
            size = 3;
            break;
        case INT:
            size = 4;
            break;
        case BIGINT:
            size = 8;
            break;
        // todo: add more data type
        default:
            throw new AssertionError(type);
        }

        return new StoneDBCompositeDataType(type, size);
    }

    public static class StoneDBTable extends
            AbstractRelationalTable<StoneDBSchema.StoneDBColumn, TableIndex, StoneDBProvider.StoneDBGlobalState> {

        public StoneDBTable(String tableName, List<StoneDBSchema.StoneDBColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static StoneDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<StoneDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<StoneDBSchema.StoneDBColumn> databaseColumns = getTableColumns(con, tableName, tableName);
            boolean isView = tableName.startsWith("v");
            StoneDBTable t = new StoneDBTable(tableName, databaseColumns, isView);
            for (StoneDBSchema.StoneDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new StoneDBSchema(databaseTables);
    }

    private static List<StoneDBColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<StoneDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isNullable = rs.getString("notnull").contentEquals("false");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    StoneDBColumn c = new StoneDBColumn(columnName, getColumnCompositeDataType(dataType), isPrimaryKey,
                            isNullable, precision);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    private static StoneDBCompositeDataType getColumnCompositeDataType(String typeString) {
        switch (typeString) {
        case "tinyint":
            return new StoneDBCompositeDataType(StoneDBDataType.TINYINT);
        case "smallint":
            return new StoneDBCompositeDataType(StoneDBDataType.SMALLINT);
        case "mediumint":
            return new StoneDBCompositeDataType(StoneDBDataType.MEDIUMINT);
        case "int":
            return new StoneDBCompositeDataType(StoneDBDataType.INT);
        case "bigint":
            return new StoneDBCompositeDataType(StoneDBDataType.BIGINT);
        case "float":
            return new StoneDBCompositeDataType(StoneDBDataType.FLOAT);
        case "double":
            return new StoneDBCompositeDataType(StoneDBDataType.DOUBLE);
        case "decimal":
            return new StoneDBCompositeDataType(StoneDBDataType.DECIMAL);
        case "char":
            return new StoneDBCompositeDataType(StoneDBDataType.CHAR);
        case "varchar":
            return new StoneDBCompositeDataType(StoneDBDataType.VARCHAR);
        // todo: support more data type
        default:
            throw new AssertionError(typeString);
        }
    }

    private static StoneDBDataType getColumnType(String typeString) {
        switch (typeString) {
        case "tinyint":
            return StoneDBDataType.TINYINT;
        case "smallint":
            return StoneDBDataType.SMALLINT;
        case "mediumint":
            return StoneDBDataType.MEDIUMINT;
        case "int":
            return StoneDBDataType.INT;
        case "bigint":
            return StoneDBDataType.BIGINT;
        case "float":
            return StoneDBDataType.FLOAT;
        case "double":
            return StoneDBDataType.DOUBLE;
        case "decimal":
            return StoneDBDataType.DECIMAL;
        case "char":
            return StoneDBDataType.CHAR;
        case "varchar":
            return StoneDBDataType.VARCHAR;
        // todo: support more data type
        default:
            throw new AssertionError(typeString);
        }
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM sqlite_master WHERE type='table' or type='view'")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("name"));
                }
            }
        }
        return tableNames;
    }

    public static class StoneDBColumn extends AbstractTableColumn<StoneDBTable, StoneDBCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;
        private final int precision;

        public StoneDBColumn(String name, StoneDBCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable,
                int precision) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
            this.precision = precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public StoneDBSchema(List<StoneDBTable> databaseTables) {
        super(databaseTables);
    }

    public static class StoneDBCompositeDataType {
        private final StoneDBDataType dataType;
        private int size;

        public StoneDBCompositeDataType(StoneDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public StoneDBCompositeDataType(StoneDBDataType dataType) {
            this.dataType = dataType;
            size = -1;
            switch (dataType) {
            case TINYINT:
                size = 1;
                break;
            case SMALLINT:
                size = 2;
                break;
            case MEDIUMINT:
                size = 3;
                break;
            case INT:
                size = 4;
                break;
            case BIGINT:
                size = 8;
                break;
            case FLOAT:
                size = 4;
                break;
            case DOUBLE:
                size = 8;
                break;
            // TODO: ADD MORE DATA TYPE
            }
        }

        public StoneDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }
    }
}