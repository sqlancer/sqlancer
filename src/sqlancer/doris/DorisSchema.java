package sqlancer.doris;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.*;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisTable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DorisSchema extends AbstractSchema<DorisGlobalState, DorisTable> {

    public enum DorisTableDataModel {
        UNIQUE, AGGREGATE, DUPLICATE;

        public static DorisTableDataModel getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum DorisColumnAggrType {
        SUM, MIN, MAX, REPLACE, REPLCAE_IF_NOT_NULL, BITMAP_UNION, HLL_UNION, NULL;

        public static DorisColumnAggrType getRandom(DorisCompositeDataType columnDataType) {
            if (columnDataType.getPrimitiveDataType() == DorisSchema.DorisDataType.BITMAP) {
                return DorisColumnAggrType.BITMAP_UNION;
            }
            if (columnDataType.getPrimitiveDataType() == DorisSchema.DorisDataType.HLL) {
                return DorisColumnAggrType.HLL_UNION;
            }

            return Randomly.fromOptions(SUM, MIN, MAX, REPLACE, REPLCAE_IF_NOT_NULL);
        }
    }

    public enum DorisDataType {
//        TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL, DECIMALV3, BOOLEAN, CHAR,
//        VARCHAR, STRING, DATE, DATEV2, DATETIME, DATETIMEV2, BITMAP, HLL, NULL;

        INT, FLOAT, DECIMAL, DATE, DATETIME, VARCHAR, BOOLEAN, HLL, BITMAP, NULL;

        public static DorisDataType getRandomWithoutNull() {
            DorisDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == DorisDataType.NULL);
            return dt;
        }

    }

    public static class DorisCompositeDataType {

        private final DorisDataType dataType;

        private final int size;

        public DorisCompositeDataType(DorisDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public DorisDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static DorisCompositeDataType getRandomWithoutNull() {
            DorisDataType type = DorisDataType.getRandomWithoutNull();
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8, 16);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 12);
                break;
            case DECIMAL:
                size = Randomly.fromOptions(1, 3);  // DECIMAL or DECIMALV3
                break;
            case DATE:
            case DATETIME:
            case VARCHAR:
            case BOOLEAN:
            case HLL:
            case BITMAP:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new DorisCompositeDataType(type, size);
        }

        @Override
        public String toString() {
            Randomly r = new Randomly();
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 16:
                    return "LARGEINT";
                case 8:
                    return "BIGINT";
                case 4:
                    return "INT";
                case 2:
                    return "SMALLINT";
                case 1:
                    return "TINYINT";
                default:
                    throw new AssertionError(size);
                }
            case FLOAT:
                switch (size) {
                    case 12:
                        return "DOUBLE";
                    case 4:
                        return "FLOAT";
                    default:
                        throw new AssertionError(size);
                }
            case DECIMAL:
                switch (size) {
                    case 1: {
                        int scale = r.getInteger(0, 9);
                        int precision = r.getInteger(scale + 1, scale + 18);
                        return "DECIMAL(" + precision + "," + scale + ")";
                    } case 3: {
                        int precision = r.getInteger(1, 38);
                        int scale = r.getInteger(0, precision);
                        return "DECIMALV3(" + precision + "," + scale + ")";
                    } default:
                        throw new AssertionError(size);
                }
            case DATE:
                return Randomly.fromOptions("DATE", "DATEV2");
            case DATETIME:
                return Randomly.fromOptions("DATETIME", "DATETIMEV2");
            case VARCHAR:
                int chars = r.getInteger(1, 255);
                return Randomly.fromOptions("VARCHAR", "CHAR") + "(" + chars + ")";
            case BOOLEAN:
                return "BOOLEAN";
            case HLL:
                return "HLL";
            case BITMAP:
                return "BITMAP";
            case NULL:
                return Randomly.fromOptions("NULL");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

        public boolean canBeKey() {
            switch (dataType) {
                case HLL:
                case BITMAP:
                case FLOAT:
                    return false;
                default:
                    return true;
            }
        }

    }

    public static class DorisColumn extends AbstractTableColumn<DorisTable, DorisCompositeDataType> {

        private final boolean isKey;
        private final boolean isNullable;
        private final DorisColumnAggrType aggrType;
        private final boolean hasDefaultValue;
        private final String defaultValue;

        public DorisColumn(String name, DorisCompositeDataType type, boolean isKey, boolean isNullable, DorisColumnAggrType aggrType, boolean hasDefaultValue, String defaultValue) {
            super(name, null, type);
            this.isKey = isKey;
            this.isNullable = isNullable;
            this.aggrType = aggrType;
            this.hasDefaultValue = hasDefaultValue;
            this.defaultValue = defaultValue;
        }

        public DorisColumn(String name, DorisCompositeDataType type, boolean isKey, boolean isNullable) {
            super(name, null, type);
            this.isKey = isKey;
            this.isNullable = isNullable;
            this.aggrType = DorisColumnAggrType.NULL;
            this.hasDefaultValue = false;
            this.defaultValue = "";
        }

        public boolean isKey() {
            return isKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

        @Override
        public String toString() {
            String ret = this.getName() + " " + this.getType();
            if (aggrType != DorisColumnAggrType.NULL) {
                ret += " " + aggrType.name();
            }
            if (!isNullable) {
                ret += " NOT NULL";
            }
            if (hasDefaultValue) {
                ret += " DEFAULT " + defaultValue;
            }
            return ret;
        }

        @Override
        public int compareTo(AbstractTableColumn<DorisTable, DorisCompositeDataType> o) {
            DorisColumn other = (DorisColumn) o;
            if (isKey != other.isKey) return isKey ? 1 : -1;
            return getName().compareTo(other.getName());
        }
    }

    public static class DorisTables extends AbstractTables<DorisTable, DorisColumn> {

        public DorisTables(List<DorisTable> tables) {
            super(tables);
        }

    }

    public DorisSchema(List<DorisTable> databaseTables) {
        super(databaseTables);
    }

    public DorisTables getRandomTableNonEmptyTables() {
        return new DorisTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static DorisCompositeDataType getColumnType(String typeString) {
        DorisDataType primitiveType;
        int size = -1;

        if (typeString.startsWith("DECIMALV3")) {
            primitiveType = DorisDataType.DECIMAL;
            size = 3;
        } else if (typeString.startsWith("DECIMAL")) {
            primitiveType = DorisDataType.DECIMAL;
            size = 1;
        } else if (typeString.startsWith("DATEV2")) {
            primitiveType = DorisDataType.DATE;
            size = 2;
        } else if (typeString.startsWith("DATE")) {
            primitiveType = DorisDataType.DATE;
            size = 1;
        }  else if (typeString.startsWith("DATETIMEV2")) {
            primitiveType = DorisDataType.DATETIME;
            size = 2;
        } else if (typeString.startsWith("DATETIME")) {
            primitiveType = DorisDataType.DATETIME;
            size = 1;
        } else if (typeString.startsWith("CHAR") || typeString.startsWith("VARCHAR")) {
            primitiveType = DorisDataType.VARCHAR;
        } else {
            switch (typeString) {
                case "LARGEINT":
                    primitiveType = DorisDataType.INT;
                    size = 16;
                    break;
                case "BITINT":
                    primitiveType = DorisDataType.INT;
                    size = 8;
                    break;
                case "INT":
                    primitiveType = DorisDataType.INT;
                    size = 4;
                    break;
                case "SMALLINT":
                    primitiveType = DorisDataType.INT;
                    size = 2;
                    break;
                case "TINYINT":
                    primitiveType = DorisDataType.INT;
                    size = 1;
                    break;
                case "DOUBLE":
                    primitiveType = DorisDataType.FLOAT;
                    size = 12;
                    break;
                case "FLOAT":
                    primitiveType = DorisDataType.FLOAT;
                    size = 4;
                    break;
                case "DECIMAL":
                case "DECIMAL(*,*)":
                    primitiveType = DorisDataType.DECIMAL;
                    size = 1;
                    break;
                case "DECIMALV3":
                case "DECIMALV3(*,*)":
                    primitiveType = DorisDataType.DECIMAL;
                    size = 3;
                    break;
                case "CHAR":
                case "CHAR(*)":
                case "VARCHAR":
                case "VARCHAR(*)":
                    primitiveType = DorisDataType.VARCHAR;
                    break;
                case "DATE":
                    primitiveType = DorisDataType.DATE;
                    size = 1;
                    break;
                case "DATEV2":
                    primitiveType = DorisDataType.DATE;
                    size = 2;
                    break;
                case "DATETIME":
                    primitiveType = DorisDataType.DATETIME;
                    size = 1;
                    break;
                case "DATETIMEV2":
                    primitiveType = DorisDataType.DATETIME;
                    size = 2;
                    break;
                case "BOOLEAN":
                    primitiveType = DorisDataType.BOOLEAN;
                    break;
                case "HLL":
                    primitiveType = DorisDataType.HLL;
                    break;
                case "BITMAP":
                    primitiveType = DorisDataType.BITMAP;
                    break;
                case "NULL":
                    primitiveType = DorisDataType.NULL;
                    break;
                default:
                    throw new AssertionError(typeString);
            }
        }
        return new DorisCompositeDataType(primitiveType, size);
    }

    public static class DorisTable extends AbstractRelationalTable<DorisColumn, TableIndex, DorisGlobalState> {

        public DorisTable(String tableName, List<DorisColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static DorisSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<DorisTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<DorisColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            DorisTable t = new DorisTable(tableName, databaseColumns, isView);
            for (DorisColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new DorisSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tableNames.add(rs.getString(1));
                }
            }
        }
        return tableNames;
    }

    private static List<DorisColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<DorisColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("DESCRIBE " + tableName)) {
                while (rs.next()) {
                    String columnName = rs.getString("Field");
                    String dataType = rs.getString("Type");
                    boolean isNullable = rs.getString("Null").contentEquals("Yes");
                    boolean isKey = rs.getString("Key").contains("true");
                    String defaultValue = rs.getString("Default");
                    boolean hasDefaultValue = (defaultValue != null);
                    DorisColumn c = new DorisColumn(columnName, getColumnType(dataType), isKey, isNullable, DorisColumnAggrType.NULL, hasDefaultValue, defaultValue);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
