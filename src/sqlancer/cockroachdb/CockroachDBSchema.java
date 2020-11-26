package sqlancer.cockroachdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;

public class CockroachDBSchema extends AbstractSchema<CockroachDBGlobalState, CockroachDBTable> {

    public enum CockroachDBDataType {

        INT, BOOL, STRING, FLOAT, BYTES, BIT, VARBIT, SERIAL, INTERVAL, TIMESTAMP, TIMESTAMPTZ, DECIMAL, JSONB, TIME,
        TIMETZ, ARRAY;

        public static CockroachDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public CockroachDBCompositeDataType get() {
            return CockroachDBCompositeDataType.getRandomForType(this);
        }

    }

    public static class CockroachDBCompositeDataType {

        private final CockroachDBDataType dataType;

        private final int size;

        private CockroachDBCompositeDataType elementType;

        public CockroachDBCompositeDataType(CockroachDBDataType dataType) {
            this.dataType = dataType;
            this.size = -1;
        }

        public CockroachDBCompositeDataType(CockroachDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public CockroachDBCompositeDataType(CockroachDBDataType dataType, CockroachDBCompositeDataType elementType) {
            if (dataType != CockroachDBDataType.ARRAY) {
                throw new IllegalArgumentException();
            }
            this.dataType = dataType;
            this.size = -1;
            this.elementType = elementType;
        }

        public CockroachDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public boolean isString() {
            return dataType == CockroachDBDataType.STRING;
        }

        public static CockroachDBCompositeDataType getInt(int size) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.INT, size);
        }

        public static CockroachDBCompositeDataType getBit(int size) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.BIT, size);
        }

        @Override
        public String toString() {
            switch (dataType) {
            case INT:
                switch (size) {
                case 2:
                    return Randomly.fromOptions("INT2", "SMALLINT");
                case 4:
                    return "INT4";
                case 8:
                    // "INTEGER": can be affected by a session variable
                    return Randomly.fromOptions("INT8", "INT64", "BIGINT");
                default:
                    return "INT";
                }
            case SERIAL:
                switch (size) {
                case 2:
                    return Randomly.fromOptions("SERIAL2", "SMALLSERIAL");
                case 4:
                    return "SERIAL4";
                case 8:
                    return Randomly.fromOptions("SERIAL8", "BIGSERIAL");
                default:
                    throw new AssertionError();
                }
            case BIT:
                if (size == 1 && Randomly.getBoolean()) {
                    return "BIT";
                } else {
                    return String.format("BIT(%d)", size);
                }
            case VARBIT:
                if (size == -1) {
                    return String.format("VARBIT");
                } else {
                    return String.format("VARBIT(%d)", size);
                }
            case ARRAY:
                return String.format("%s[]", elementType.toString());
            default:
                return dataType.toString();
            }
        }

        public static CockroachDBCompositeDataType getRandom() {
            CockroachDBDataType randomDataType = CockroachDBDataType.getRandom();
            return getRandomForType(randomDataType);
        }

        private static CockroachDBCompositeDataType getRandomForType(CockroachDBDataType randomDataType) {
            if (randomDataType == CockroachDBDataType.INT || randomDataType == CockroachDBDataType.SERIAL) {
                return new CockroachDBCompositeDataType(randomDataType, Randomly.fromOptions(2, 4, 8));
            } else if (randomDataType == CockroachDBDataType.BIT) {
                return new CockroachDBCompositeDataType(randomDataType, (int) Randomly.getNotCachedInteger(1, 200));
            } else if (randomDataType == CockroachDBDataType.VARBIT) {
                return new CockroachDBCompositeDataType(randomDataType, (int) Randomly.getNotCachedInteger(1, 200));
            } else if (randomDataType == CockroachDBDataType.ARRAY) {
                return new CockroachDBCompositeDataType(randomDataType, getRandomForType(getArrayElementType()));
            } else {
                return new CockroachDBCompositeDataType(randomDataType);
            }
        }

        private static CockroachDBDataType getArrayElementType() {
            while (true) {
                CockroachDBDataType type = CockroachDBDataType.getRandom();
                if (type != CockroachDBDataType.ARRAY && type != CockroachDBDataType.JSONB) {
                    // nested arrays are not supported:
                    // https://github.com/cockroachdb/cockroach/issues/32552
                    // JSONB arrays are not supported as well:
                    // https://github.com/cockroachdb/cockroach/issues/23468
                    return type;
                }
            }
        }

        public static CockroachDBCompositeDataType getVarBit(int maxSize) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.VARBIT, maxSize);
        }

        public CockroachDBCompositeDataType getElementType() {
            return elementType;
        }

    }

    public static class CockroachDBColumn extends AbstractTableColumn<CockroachDBTable, CockroachDBCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public CockroachDBColumn(String name, CockroachDBCompositeDataType columnType, boolean isPrimaryKey,
                boolean isNullable) {
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

    public static class CockroachDBTables extends AbstractTables<CockroachDBTable, CockroachDBColumn> {

        public CockroachDBTables(List<CockroachDBTable> tables) {
            super(tables);
        }

    }

    public CockroachDBSchema(List<CockroachDBTable> databaseTables) {
        super(databaseTables);
    }

    public CockroachDBTables getRandomTableNonEmptyTables() {
        return new CockroachDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static CockroachDBCompositeDataType getColumnType(String typeString) {
        if (typeString.endsWith("[]")) {
            String substring = typeString.substring(0, typeString.length() - 2);
            CockroachDBCompositeDataType elementType = getColumnType(substring);
            return new CockroachDBCompositeDataType(CockroachDBDataType.ARRAY, elementType);
        }
        if (typeString.startsWith("STRING COLLATE")) {
            return new CockroachDBCompositeDataType(CockroachDBDataType.STRING);
        }
        if (typeString.startsWith("BIT(")) {
            int val = Integer.parseInt(typeString.substring(4, typeString.length() - 1));
            return CockroachDBCompositeDataType.getBit(val);
        }
        if (typeString.startsWith("VARBIT(")) {
            int val = Integer.parseInt(typeString.substring(7, typeString.length() - 1));
            return CockroachDBCompositeDataType.getBit(val);
        }
        switch (typeString) {
        case "VARBIT":
            return CockroachDBCompositeDataType.getVarBit(-1);
        case "BIT":
            return CockroachDBCompositeDataType.getBit(1);
        case "INT8":
            return CockroachDBCompositeDataType.getInt(8);
        case "INT4":
            return CockroachDBCompositeDataType.getInt(4);
        case "INT2":
            return CockroachDBCompositeDataType.getInt(2);
        case "BOOL":
            return new CockroachDBCompositeDataType(CockroachDBDataType.BOOL);
        case "STRING":
            return new CockroachDBCompositeDataType(CockroachDBDataType.STRING);
        case "FLOAT8":
            return new CockroachDBCompositeDataType(CockroachDBDataType.FLOAT);
        case "BYTES":
            return new CockroachDBCompositeDataType(CockroachDBDataType.BYTES);
        case "INTERVAL":
            return new CockroachDBCompositeDataType(CockroachDBDataType.INTERVAL);
        case "DECIMAL":
            return new CockroachDBCompositeDataType(CockroachDBDataType.DECIMAL);
        case "TIMESTAMP":
            return new CockroachDBCompositeDataType(CockroachDBDataType.TIMESTAMP);
        case "TIMESTAMPTZ":
            return new CockroachDBCompositeDataType(CockroachDBDataType.TIMESTAMPTZ);
        case "JSONB":
            return new CockroachDBCompositeDataType(CockroachDBDataType.JSONB);
        case "TIME":
            return new CockroachDBCompositeDataType(CockroachDBDataType.TIME);
        case "TIMETZ":
            return new CockroachDBCompositeDataType(CockroachDBDataType.TIMETZ);

        default:
            throw new AssertionError(typeString);
        }
    }

    public static class CockroachDBTable
            extends AbstractRelationalTable<CockroachDBColumn, TableIndex, CockroachDBGlobalState> {

        public CockroachDBTable(String tableName, List<CockroachDBColumn> columns, List<TableIndex> indexes,
                boolean isView) {
            super(tableName, columns, indexes, isView);
        }

    }

    public static CockroachDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<CockroachDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<CockroachDBColumn> databaseColumns = getTableColumns(con, tableName);
            List<TableIndex> indexes = getIndexes(con, tableName);
            boolean isView = tableName.startsWith("v");
            CockroachDBTable t = new CockroachDBTable(tableName, databaseColumns, indexes, isView);
            for (CockroachDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new CockroachDBSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            ResultSet tableRs = s.executeQuery(
                    "SELECT table_name FROM information_schema.tables WHERE TABLE_TYPE IN ('BASE TABLE', 'LOCAL TEMPORARY');");
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
                    String indexName = rs.getString("index_name");
                    indexes.add(TableIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<CockroachDBColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<CockroachDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SHOW COLUMNS FROM " + tableName)) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    if (columnName.contains("crdb_internal")) {
                        continue; // created for CREATE INDEX ON t0(c0) USING HASH WITH BUCKET_COUNT = 1;
                    }
                    String dataType = rs.getString("data_type");
                    boolean isNullable = rs.getBoolean("is_nullable");
                    String indices = rs.getString("indices");
                    boolean isPrimaryKey = indices.contains("primary");
                    CockroachDBColumn c = new CockroachDBColumn(columnName, getColumnType(dataType), isPrimaryKey,
                            isNullable);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
