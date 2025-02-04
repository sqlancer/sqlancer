package sqlancer.presto;

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

public class PrestoSchema extends AbstractSchema<PrestoGlobalState, PrestoSchema.PrestoTable> {

    public PrestoSchema(List<PrestoTable> databaseTables) {
        super(databaseTables);
    }

    public static PrestoSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<PrestoTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<PrestoColumn> databaseColumns = getTableColumns(con, databaseName, tableName);
            boolean isView = tableName.startsWith("v");
            PrestoTable t = new PrestoTable(tableName, databaseColumns, isView);
            for (PrestoColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);
        }
        return new PrestoSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            // TODO: UPDATE
            // SHOW TABLES [ FROM schema ] [ LIKE pattern [ ESCAPE 'escape_character' ] ]
            try (ResultSet rs = s.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("Table"));
                }
            }
        }
        return tableNames;
    }

    private static List<PrestoColumn> getTableColumns(SQLConnection con, String databaseName, String tableName)
            throws SQLException {
        List<PrestoColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("select " + " table_catalog " + " , table_schema "
                    + " , table_name " + " , column_name " + " , is_nullable " + " , data_type "
                    + " from information_schema.columns " + " where table_schema = '%s' and table_name = '%s'",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    boolean isNullable = rs.getString("is_nullable").contentEquals("YES");
                    PrestoColumn c = new PrestoColumn(columnName, getColumnType(dataType), false, isNullable);
                    columns.add(c);
                }
            }
        }

        return columns;
    }

    private static PrestoCompositeDataType getColumnType(String typeString) {
        int bracesStart = typeString.indexOf('(');
        String type;
        int size = 0;
        int precision = 0;
        if (bracesStart != -1) {
            type = typeString.substring(0, bracesStart);
        } else {
            type = typeString;
        }
        type = type.toUpperCase();

        PrestoDataType primitiveType;
        switch (type) {
        case "INTEGER":
            primitiveType = PrestoDataType.INT;
            size = 4;
            break;
        case "SMALLINT":
            primitiveType = PrestoDataType.INT;
            size = 2;
            break;
        case "BIGINT":
            primitiveType = PrestoDataType.INT;
            size = 8;
            break;
        case "TINYINT":
            primitiveType = PrestoDataType.INT;
            size = 1;
            break;
        case "VARCHAR":
            primitiveType = PrestoDataType.VARCHAR;
            break;
        case "VARBINARY":
            primitiveType = PrestoDataType.VARBINARY;
            break;
        case "CHAR":
            primitiveType = PrestoDataType.CHAR;
            break;
        case "FLOAT":
        case "REAL":
            primitiveType = PrestoDataType.FLOAT;
            size = 4;
            break;
        case "DOUBLE":
            primitiveType = PrestoDataType.FLOAT;
            size = 8;
            break;
        case "DECIMAL":
            primitiveType = PrestoDataType.DECIMAL;
            break;
        case "BOOLEAN":
            primitiveType = PrestoDataType.BOOLEAN;
            break;
        case "DATE":
            primitiveType = PrestoDataType.DATE;
            break;
        case "TIME":
            primitiveType = PrestoDataType.TIME;
            break;
        case "TIME WITH TIME ZONE":
            primitiveType = PrestoDataType.TIME_WITH_TIME_ZONE;
            break;
        case "TIMESTAMP":
            primitiveType = PrestoDataType.TIMESTAMP;
            break;
        case "TIMESTAMP WITH TIME ZONE":
            primitiveType = PrestoDataType.TIMESTAMP_WITH_TIME_ZONE;
            break;
        case "INTERVAL DAY TO SECOND":
            primitiveType = PrestoDataType.INTERVAL_DAY_TO_SECOND;
            break;
        case "INTERVAL YEAR TO MONTH":
            primitiveType = PrestoDataType.INTERVAL_YEAR_TO_MONTH;
            break;
        case "JSON":
            primitiveType = PrestoDataType.JSON;
            break;
        case "ARRAY":
            primitiveType = PrestoDataType.ARRAY;
            break;
        case "NULL":
            primitiveType = PrestoDataType.NULL;
            break;
        default:
            throw new AssertionError(typeString);
        }
        return new PrestoCompositeDataType(primitiveType, size, precision);
    }

    public PrestoTables getRandomTableNonEmptyTables() {
        return new PrestoTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public enum PrestoDataType {
        BOOLEAN, INT, FLOAT, DECIMAL, VARCHAR, CHAR, VARBINARY, JSON, DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE,
        TIMESTAMP_WITH_TIME_ZONE, INTERVAL_YEAR_TO_MONTH, INTERVAL_DAY_TO_SECOND, ARRAY,
        // MAP,
        // ROW,
        // IPADDRESS,
        // UID,
        // IPPREFIX,
        // HyperLogLog,
        // P4HyperLogLog,
        // KHyperLogLog,
        // QDigest,
        // TDigest,
        NULL;

        public static PrestoDataType getRandomWithoutNull() {
            PrestoDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == PrestoDataType.NULL);
            return dt;
        }

        public static List<PrestoDataType> getNumericTypes() {
            return Arrays.asList(INT, FLOAT, DECIMAL, DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE,
                    TIMESTAMP_WITH_TIME_ZONE);
        }

        public static List<PrestoDataType> getComparableTypes() {
            return Arrays.asList(BOOLEAN, INT, FLOAT, DECIMAL, VARCHAR, CHAR, VARBINARY, JSON, DATE, TIME, TIMESTAMP,
                    TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, INTERVAL_YEAR_TO_MONTH, INTERVAL_DAY_TO_SECOND);
        }

        public static List<PrestoDataType> getOrderableTypes() {
            return Arrays.asList(BOOLEAN, INT, FLOAT, DECIMAL, VARCHAR, CHAR, VARBINARY,
                    // JSON,
                    DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, INTERVAL_YEAR_TO_MONTH,
                    INTERVAL_DAY_TO_SECOND, ARRAY);
        }

        public static List<PrestoDataType> getNumberTypes() {
            return Arrays.asList(INT, FLOAT, DECIMAL);
        }

        public static List<PrestoDataType> getTemporalTypes() {
            return Arrays.asList(DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE);
        }

        public static List<PrestoDataType> getIntervalTypes() {
            return Arrays.asList(INTERVAL_YEAR_TO_MONTH, INTERVAL_DAY_TO_SECOND);
        }

        public static List<PrestoDataType> getTextTypes() {
            return Arrays.asList(VARCHAR, CHAR, VARBINARY, JSON);
        }

        public boolean isNumeric() {
            switch (this) {
            case INT:
            case FLOAT:
            case DECIMAL:
                return true;
            default:
                return false;
            }
        }

        public boolean isOrderable() {
            return getOrderableTypes().contains(this);
        }

        public PrestoCompositeDataType get() {
            return PrestoCompositeDataType.fromDataType(this);
        }
    }

    public static class PrestoCompositeDataType {

        private final PrestoDataType dataType;

        private final int size;

        private final int scale;

        private final PrestoCompositeDataType elementType;

        public PrestoCompositeDataType(PrestoDataType dataType, int dataSize, int dataScale) {
            this.dataType = dataType;
            this.size = dataSize;
            this.scale = dataScale;
            this.elementType = null;
        }

        public PrestoCompositeDataType(PrestoDataType dataType, PrestoCompositeDataType elementType) {
            if (dataType != PrestoDataType.ARRAY) {
                throw new IllegalArgumentException();
            }
            this.dataType = dataType;
            this.size = -1;
            this.scale = -1;
            this.elementType = elementType;
        }

        public static PrestoCompositeDataType getRandomWithoutNull() {
            PrestoDataType type = PrestoDataType.getRandomWithoutNull();
            int size;
            int scale = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 8);
                break;
            case DECIMAL:
                size = Math.toIntExact(8);
                scale = Math.toIntExact(4);
                break;
            case VARBINARY:
            case JSON:
            case VARCHAR:
            case CHAR:
                size = Math.toIntExact(Randomly.getNotCachedInteger(10, 250));
                break;
            case ARRAY:
                return new PrestoCompositeDataType(type, PrestoCompositeDataType.getRandomWithoutNull());
            case BOOLEAN:
            case DATE:
            case TIME:
            case TIME_WITH_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case INTERVAL_DAY_TO_SECOND:
            case INTERVAL_YEAR_TO_MONTH:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new PrestoCompositeDataType(type, size, scale);
        }

        public static PrestoCompositeDataType fromDataType(PrestoDataType type) {
            int size;
            int scale = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 8);
                break;
            case DECIMAL:
                size = Math.toIntExact(8);
                scale = Math.toIntExact(4);
                break;
            case JSON:
            case VARCHAR:
            case CHAR:
                size = Math.toIntExact(Randomly.getNotCachedInteger(10, 250));
                break;
            case ARRAY:
                return new PrestoCompositeDataType(type, PrestoCompositeDataType.getRandomWithoutNull());
            case BOOLEAN:
            case VARBINARY:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIME_WITH_TIME_ZONE:
            case INTERVAL_DAY_TO_SECOND:
            case INTERVAL_YEAR_TO_MONTH:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new PrestoCompositeDataType(type, size, scale);
        }

        public PrestoDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public int getScale() {
            if (scale == -1) {
                throw new AssertionError(this);
            }
            return scale;
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return "BIGINT";
                case 4:
                    return "INTEGER";
                case 2:
                    return "SMALLINT";
                case 1:
                    return "TINYINT";
                default:
                    throw new AssertionError(size);
                }
            case VARBINARY:
                return "VARBINARY";
            case JSON:
                return "JSON";
            case VARCHAR:
                return "VARCHAR" + "(" + size + ")";
            case CHAR:
                return "CHAR" + "(" + size + ")";
            case FLOAT:
                switch (size) {
                case 4:
                    return "REAL";
                case 8:
                    return "DOUBLE";
                default:
                    throw new AssertionError(size);
                }
            case DECIMAL:
                return "DECIMAL" + "(" + size + ", " + scale + ")";
            case BOOLEAN:
                return "BOOLEAN";
            case TIMESTAMP_WITH_TIME_ZONE:
                return "TIMESTAMP WITH TIME ZONE";
            case TIMESTAMP:
                return "TIMESTAMP";
            case INTERVAL_YEAR_TO_MONTH:
                return "INTERVAL YEAR TO MONTH";
            case INTERVAL_DAY_TO_SECOND:
                return "INTERVAL DAY TO SECOND";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIME_WITH_TIME_ZONE:
                return "TIME WITH TIME ZONE";
            case ARRAY:
                return "ARRAY(" + elementType + ")";
            case NULL:
                return "NULL";
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

        public PrestoCompositeDataType getElementType() {
            return elementType;
        }

        public boolean isOrderable() {
            if (dataType == PrestoDataType.ARRAY) {
                assert elementType != null;
                return elementType.isOrderable();
            }
            return dataType.isOrderable();
        }

    }

    public static class PrestoColumn extends AbstractTableColumn<PrestoTable, PrestoCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public PrestoColumn(String name, PrestoCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
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

        public boolean isOrderable() {
            return getType().getPrimitiveDataType().isOrderable();
        }

    }

    public static class PrestoTables extends AbstractTables<PrestoTable, PrestoColumn> {

        public PrestoTables(List<PrestoTable> tables) {
            super(tables);
        }

    }

    public static class PrestoTable extends AbstractRelationalTable<PrestoColumn, TableIndex, PrestoGlobalState> {

        public PrestoTable(String tableName, List<PrestoColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

}
