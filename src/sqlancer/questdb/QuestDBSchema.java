package sqlancer.questdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;

public class QuestDBSchema extends AbstractSchema<QuestDBGlobalState, QuestDBTable> {

    public enum QuestDBDataType {

        BOOLEAN, // CHAR,
        /* STRING, */
        INT, FLOAT,
        /* SYMBOL, */
        // DATE, TIMESTAMP,
        /* GEOHASH, */
        NULL;

        public static QuestDBDataType getRandomWithoutNull() {
            QuestDBDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == QuestDBDataType.NULL);
            return dt;
        }

    }

    public static class QuestDBCompositeDataType {

        private final QuestDBDataType dataType;

        private final int size;

        private final boolean isNullable;

        public QuestDBCompositeDataType(QuestDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;

            switch (dataType) {
            case INT:
                switch (size) {
                case 1:
                case 2:
                    isNullable = false;
                    break;
                default:
                    isNullable = true;
                    break;
                }
                break;
            case BOOLEAN:
                isNullable = false;
                break;
            default:
                isNullable = true;
            }
        }

        public QuestDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public static QuestDBCompositeDataType getRandomWithoutNull() {
            QuestDBDataType type = QuestDBDataType.getRandomWithoutNull();
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 8, 32);
                break;
            case BOOLEAN:
                // case CHAR:
                // case DATE:
                // case TIMESTAMP:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new QuestDBCompositeDataType(type, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 1:
                    return Randomly.fromOptions("BYTE");
                case 2:
                    return Randomly.fromOptions("SHORT");
                case 4:
                    return Randomly.fromOptions("INT");
                default:
                    throw new AssertionError(size);
                }
                // case CHAR:
                // return "CHAR";
            case FLOAT:
                switch (size) {
                case 4:
                    return Randomly.fromOptions("FLOAT");
                case 8:
                    return Randomly.fromOptions(/* "DOUBLE", */"LONG");
                case 32:
                    return Randomly.fromOptions("LONG256");
                default:
                    throw new AssertionError(size);
                }
            case BOOLEAN:
                return Randomly.fromOptions("BOOLEAN");
            // case TIMESTAMP:
            // return Randomly.fromOptions("TIMESTAMP");
            // case DATE:
            // return Randomly.fromOptions("DATE");
            case NULL:
                return Randomly.fromOptions("NULL");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class QuestDBColumn extends AbstractTableColumn<QuestDBTable, QuestDBCompositeDataType> {
        private final boolean isIndexed;
        private final boolean isNullable;

        public QuestDBColumn(String name, QuestDBCompositeDataType columnType, boolean isIndexed) {
            super(name, null, columnType);
            this.isIndexed = isIndexed;
            this.isNullable = columnType == null || columnType.isNullable();
        }

        public boolean isIndexed() {
            return isIndexed;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class QuestDBTables extends AbstractTables<QuestDBTable, QuestDBColumn> {
        public static final Set<String> RESERVED_TABLES = new HashSet<>(
                Arrays.asList("sys.column_versions_purge_log", "telemetry_config", "telemetry"));

        public QuestDBTables(List<QuestDBTable> tables) {
            super(tables);
        }
    }

    public QuestDBSchema(List<QuestDBTable> databaseTables) {
        super(databaseTables);
    }

    public QuestDBTables getRandomTableNonEmptyTables() {
        return new QuestDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static QuestDBCompositeDataType getColumnType(String typeString) {
        QuestDBDataType primitiveType;
        int size = -1;

        switch (typeString) {
        case "INT":
            primitiveType = QuestDBDataType.INT;
            size = 4;
            break;
        // case "CHAR":
        // primitiveType = QuestDBDataType.CHAR;
        // break;
        case "FLOAT":
            primitiveType = QuestDBDataType.FLOAT;
            size = 4;
            break;
        case "LONG":
            primitiveType = QuestDBDataType.FLOAT;
            size = 8;
            break;
        case "LONG256":
            primitiveType = QuestDBDataType.FLOAT;
            size = 32;
            break;
        case "BOOLEAN":
            primitiveType = QuestDBDataType.BOOLEAN;
            break;
        // case "DATE":
        // primitiveType = QuestDBDataType.DATE;
        // break;
        // case "TIMESTAMP":
        // primitiveType = QuestDBDataType.TIMESTAMP;
        // break;
        case "BYTE":
            primitiveType = QuestDBDataType.INT;
            size = 1;
            break;
        case "SHORT":
            primitiveType = QuestDBDataType.INT;
            size = 2;
            break;
        case "NULL":
            primitiveType = QuestDBDataType.NULL;
            break;
        default:
            throw new AssertionError(typeString);
        }
        return new QuestDBCompositeDataType(primitiveType, size);
    }

    public static class QuestDBTable extends AbstractRelationalTable<QuestDBColumn, TableIndex, QuestDBGlobalState> {

        public QuestDBTable(String tableName, List<QuestDBColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static QuestDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<QuestDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<QuestDBColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            QuestDBTable t = new QuestDBTable(tableName, databaseColumns, isView);
            for (QuestDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new QuestDBSchema(databaseTables);
    }

    protected static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SHOW TABLES;")) {
                while (rs.next()) {
                    String tName = rs.getString("table");
                    // exclude reserved tables for testing
                    if (!QuestDBTables.RESERVED_TABLES.contains(tName)) {
                        tableNames.add(tName);
                    }
                }
            }
        }
        return tableNames;
    }

    private static List<QuestDBColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<QuestDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SHOW COLUMNS FROM %s;", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("column");
                    String dataType = rs.getString("type");
                    boolean isIndexed = rs.getString("indexed").contains("true");
                    QuestDBColumn c = new QuestDBColumn(columnName, getColumnType(dataType), isIndexed);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
