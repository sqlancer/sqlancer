package sqlancer.yugabyte.ycql;

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
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;

public class YCQLSchema extends AbstractSchema<YCQLGlobalState, YCQLTable> {

    public enum YCQLDataType {

        INT, VARCHAR, BOOLEAN, FLOAT, DATE, TIMESTAMP;

        public static YCQLDataType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static class YCQLCompositeDataType {

        private final YCQLDataType dataType;

        private final int size;

        public YCQLCompositeDataType(YCQLDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public YCQLDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static YCQLCompositeDataType getRandom() {
            YCQLDataType type = YCQLDataType.getRandom();
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

            return new YCQLCompositeDataType(type, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("BIGINT");
                case 4:
                    return Randomly.fromOptions("INTEGER", "INT");
                case 2:
                    return Randomly.fromOptions("SMALLINT");
                case 1:
                    return Randomly.fromOptions("TINYINT");
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return "VARCHAR";
            case FLOAT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("DOUBLE");
                case 4:
                    return Randomly.fromOptions("FLOAT");
                default:
                    throw new AssertionError(size);
                }
            case BOOLEAN:
                return Randomly.fromOptions("BOOLEAN");
            case TIMESTAMP:
                return Randomly.fromOptions("TIMESTAMP");
            case DATE:
                return Randomly.fromOptions("DATE");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class YCQLColumn extends AbstractTableColumn<YCQLTable, YCQLCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public YCQLColumn(String name, YCQLCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
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

    public static class YCQLTables extends AbstractTables<YCQLTable, YCQLColumn> {

        public YCQLTables(List<YCQLTable> tables) {
            super(tables);
        }

    }

    public YCQLSchema(List<YCQLTable> databaseTables) {
        super(databaseTables);
    }

    public YCQLTables getRandomTableNonEmptyTables() {
        return new YCQLTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static YCQLCompositeDataType getColumnType(String typeString) {
        YCQLDataType primitiveType;
        int size = -1;
        switch (typeString.toUpperCase()) {
        case "INT":
        case "INTEGER":
            primitiveType = YCQLDataType.INT;
            size = 4;
            break;
        case "SMALLINT":
            primitiveType = YCQLDataType.INT;
            size = 2;
            break;
        case "BIGINT":
            primitiveType = YCQLDataType.INT;
            size = 8;
            break;
        case "TINYINT":
            primitiveType = YCQLDataType.INT;
            size = 1;
            break;
        case "VARCHAR":
        case "TEXT":
            primitiveType = YCQLDataType.VARCHAR;
            break;
        case "FLOAT":
            primitiveType = YCQLDataType.FLOAT;
            size = 4;
            break;
        case "DOUBLE":
            primitiveType = YCQLDataType.FLOAT;
            size = 8;
            break;
        case "BOOLEAN":
            primitiveType = YCQLDataType.BOOLEAN;
            break;
        case "DATE":
            primitiveType = YCQLDataType.DATE;
            break;
        case "TIMESTAMP":
            primitiveType = YCQLDataType.TIMESTAMP;
            break;
        default:
            throw new AssertionError();
        }
        return new YCQLCompositeDataType(primitiveType, size);
    }

    public static class YCQLTable extends AbstractRelationalTable<YCQLColumn, TableIndex, YCQLGlobalState> {

        public YCQLTable(String tableName, List<YCQLColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static YCQLSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<YCQLTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con, databaseName);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue;
            }
            List<YCQLColumn> databaseColumns = getTableColumns(con, databaseName, tableName);
            boolean isView = tableName.startsWith("v");
            YCQLTable t = new YCQLTable(tableName, databaseColumns, isView);
            for (YCQLColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new YCQLSchema(databaseTables);
    }

    public static List<String> getTableNames(SQLConnection con, String databaseName) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    String.format("select * from system_schema.tables where keyspace_name = '%s'", databaseName))) {
                while (rs.next()) {
                    tableNames.add(rs.getString("table_name"));
                }
            }
        }
        return tableNames;
    }

    private static List<YCQLColumn> getTableColumns(SQLConnection con, String databaseName, String tableName)
            throws SQLException {
        List<YCQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "select * from system_schema.columns where keyspace_name = '%s' and table_name = '%s'",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("type");
                    boolean isPrimaryKey = rs.getString("kind").contentEquals("partition_key");
                    YCQLColumn c = new YCQLColumn(columnName, getColumnType(dataType), isPrimaryKey, true);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
