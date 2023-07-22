package sqlancer.doris;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.ast.DorisConstant;

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
            // if (columnDataType.getPrimitiveDataType() == DorisSchema.DorisDataType.BITMAP) {
            // return DorisColumnAggrType.BITMAP_UNION;
            // }
            // if (columnDataType.getPrimitiveDataType() == DorisSchema.DorisDataType.HLL) {
            // return DorisColumnAggrType.HLL_UNION;
            // }

            return Randomly.fromOptions(SUM, MIN, MAX, REPLACE, REPLCAE_IF_NOT_NULL);
        }
    }

    public enum DorisDataType {
        INT, FLOAT, DECIMAL, DATE, DATETIME, VARCHAR, BOOLEAN, NULL;
        // HLL, BITMAP, ARRAY;

        private int decimalScale;
        private int decimalPrecision;
        private int varcharLength;

        public static DorisDataType getRandomWithoutNull() {
            DorisDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == DorisDataType.NULL);
            return dt;
        }

        public static DorisDataType getRandomWithoutNull(DorisOptions options) {
            List<DorisDataType> validTypes = new ArrayList<>();
            if (options.testIntConstants) {
                validTypes.add(DorisDataType.INT);
            }
            if (options.testFloatConstants) {
                validTypes.add(DorisDataType.FLOAT);
            }
            if (options.testDecimalConstants) {
                validTypes.add(DorisDataType.DECIMAL);
            }
            if (options.testDateConstants) {
                validTypes.add(DorisDataType.DATE);
            }
            if (options.testDateTimeConstants) {
                validTypes.add(DorisDataType.DATETIME);
            }
            if (options.testStringConstants) {
                validTypes.add(DorisDataType.VARCHAR);
            }
            if (options.testBooleanConstants) {
                validTypes.add(DorisDataType.BOOLEAN);
            }

            return Randomly.fromList(validTypes);
        }

        public int getDecimalScale() {
            return decimalScale;
        }

        public void setDecimalScale(int decimalScale) {
            this.decimalScale = decimalScale;
        }

        public int getDecimalPrecision() {
            return decimalPrecision;
        }

        public void setDecimalPrecision(int decimalPrecision) {
            this.decimalPrecision = decimalPrecision;
        }

        public int getVarcharLength() {
            return varcharLength;
        }

        public void setVarcharLength(int varcharLength) {
            this.varcharLength = varcharLength;
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

        public static DorisCompositeDataType getRandomWithoutNull(DorisGlobalState globalState) {
            DorisDataType type = DorisDataType.getRandomWithoutNull(globalState.getDbmsSpecificOptions());
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8, 16);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 12);
                break;
            case DECIMAL:
                size = Randomly.fromOptions(1, 3); // DECIMAL or DECIMALV3
                break;
            case DATE:
            case DATETIME:
            case VARCHAR:
            case BOOLEAN:
                // case HLL:
                // case BITMAP:
                // case ARRAY:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new DorisCompositeDataType(type, size);
        }

        public void initColumnArgs() {
            Randomly r = new Randomly();
            int scale;
            int precision;
            int varcharLength;
            switch (getPrimitiveDataType()) {
            case DECIMAL:
                if (getPrimitiveDataType().getDecimalPrecision() != 0) {
                    break;
                }
                if (size == 1) {
                    scale = r.getInteger(0, 9);
                    precision = r.getInteger(scale + 1, scale + 18);
                    getPrimitiveDataType().setDecimalPrecision(precision);
                    getPrimitiveDataType().setDecimalScale(scale);
                } else if (size == 3) {
                    precision = r.getInteger(1, 38);
                    scale = r.getInteger(0, precision);
                    getPrimitiveDataType().setDecimalPrecision(precision);
                    getPrimitiveDataType().setDecimalScale(scale);
                } else {
                    throw new AssertionError(size);
                }
                break;
            case VARCHAR:
                if (getPrimitiveDataType().getVarcharLength() != 0) {
                    break;
                }
                varcharLength = r.getInteger(1, 255);
                getPrimitiveDataType().setVarcharLength(varcharLength);
                break;
            default:
                // pass
            }

        }

        @Override
        public String toString() {
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
                case 1:
                    return "DECIMAL(" + getPrimitiveDataType().getDecimalPrecision() + ","
                            + getPrimitiveDataType().getDecimalScale() + ")";
                case 3:
                    return "DECIMALV3(" + getPrimitiveDataType().getDecimalPrecision() + ","
                            + getPrimitiveDataType().getDecimalScale() + ")";
                default:
                    throw new AssertionError(size);
                }
            case DATE:
                return "DATEV2";
            case DATETIME:
                return Randomly.fromOptions("DATETIME", "DATETIMEV2");
            case VARCHAR:
                return Randomly.fromOptions("VARCHAR", "CHAR") + "(" + getPrimitiveDataType().getVarcharLength() + ")";
            case BOOLEAN:
                return "BOOLEAN";
            // case HLL:
            // return "HLL";
            // case BITMAP:
            // return "BITMAP";
            // case ARRAY:
            // return "ARRAY";
            case NULL:
                return Randomly.fromOptions("NULL");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

        public boolean canBeKey() {
            switch (dataType) {
            // case HLL:
            // case BITMAP:
            // case ARRAY:
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

        public DorisColumn(String name, DorisCompositeDataType type, boolean isKey, boolean isNullable,
                DorisColumnAggrType aggrType, boolean hasDefaultValue, String defaultValue) {
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

        public boolean hasDefaultValue() {
            return hasDefaultValue;
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
            // To sort columns
            DorisColumn other = (DorisColumn) o;
            if (isKey != other.isKey) {
                return isKey ? 1 : -1;
            }
            return getName().compareTo(other.getName());
        }
    }

    public static class DorisTables extends AbstractTables<DorisTable, DorisColumn> {

        public DorisTables(List<DorisTable> tables) {
            super(tables);
        }

        public DorisRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String rowValueQuery = String.format("SELECT %s FROM %s ORDER BY 1 LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    tableNamesAsString());
            Map<DorisColumn, DorisConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet rs = s.executeQuery(rowValueQuery);
                if (!rs.next()) {
                    throw new IgnoreMeException();
                    // throw new AssertionError("could not find random row " + rowValueQuery + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    DorisColumn column = getColumns().get(i);
                    int columnIndex = rs.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    DorisConstant constant;
                    if (rs.getString(columnIndex) == null) {
                        constant = DorisConstant.createNullConstant();
                    } else {
                        switch (column.getType().getPrimitiveDataType()) {
                        case INT:
                            constant = DorisConstant.createIntConstant(rs.getLong(columnIndex));
                            break;
                        case FLOAT:
                        case DECIMAL:
                            constant = DorisConstant.createFloatConstant(rs.getDouble(columnIndex));
                            break;
                        case DATE:
                            constant = DorisConstant.createDateConstant(rs.getString(columnIndex));
                            break;
                        case DATETIME:
                            constant = DorisConstant.createDatetimeConstant(rs.getString(columnIndex));
                            break;
                        case VARCHAR:
                            constant = DorisConstant.createStringConstant(rs.getString(columnIndex));
                            break;
                        case BOOLEAN:
                            constant = DorisConstant.createBooleanConstant(rs.getBoolean(columnIndex));
                            break;
                        case NULL:
                            constant = DorisConstant.createNullConstant();
                            break;
                        default:
                            throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                assert !rs.next();
                return new DorisRowValue(this, values);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        }

    }

    public static class DorisRowValue extends AbstractRowValue<DorisTables, DorisColumn, DorisConstant> {

        DorisRowValue(DorisTables tables, Map<DorisColumn, DorisConstant> values) {
            super(tables, values);
        }

    }

    public DorisSchema(List<DorisTable> databaseTables) {
        super(databaseTables);
    }

    public DorisTables getRandomTableNonEmptyTables() {
        return new DorisTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public DorisTables getRandomTableNonEmptyAndViewTables() {
        List<DorisTable> tables = getDatabaseTables().stream().filter(t -> !t.isView()).collect(Collectors.toList());
        tables = Randomly.nonEmptySubset(tables);
        return new DorisTables(tables);
    }

    public int getIndexCount() {
        int count = 0;
        for (DorisTable table : getDatabaseTables()) {
            count += table.getIndexes().size();
        }
        return count;
    }

    private static DorisCompositeDataType getColumnType(String typeString) {
        DorisDataType primitiveType;
        int size = -1;

        if (typeString.startsWith("DECIMALV3")) {
            primitiveType = DorisDataType.DECIMAL;
            String precisionAndScale = typeString.substring(typeString.indexOf('(') + 1, typeString.indexOf(')'));
            String[] split = precisionAndScale.split(",");
            assert split.length == 2;
            primitiveType.setDecimalPrecision(Integer.parseInt(split[0].trim()));
            primitiveType.setDecimalScale(Integer.parseInt(split[1].trim()));
            size = 3;
        } else if (typeString.startsWith("DECIMAL")) {
            primitiveType = DorisDataType.DECIMAL;
            String precisionAndScale = typeString.substring(typeString.indexOf('(') + 1, typeString.indexOf(')'));
            String[] split = precisionAndScale.split(",");
            assert split.length == 2;
            primitiveType.setDecimalPrecision(Integer.parseInt(split[0].trim()));
            primitiveType.setDecimalScale(Integer.parseInt(split[1].trim()));
            size = 1;
        } else if (typeString.startsWith("DATEV2")) {
            primitiveType = DorisDataType.DATE;
            size = 2;
        } else if (typeString.startsWith("DATE")) {
            primitiveType = DorisDataType.DATE;
            size = 1;
        } else if (typeString.startsWith("DATETIMEV2")) {
            primitiveType = DorisDataType.DATETIME;
            size = 2;
        } else if (typeString.startsWith("DATETIME")) {
            primitiveType = DorisDataType.DATETIME;
            size = 1;
        } else if (typeString.startsWith("CHAR") || typeString.startsWith("VARCHAR")) {
            primitiveType = DorisDataType.VARCHAR;
            String varcharLength = typeString.substring(typeString.indexOf('(') + 1, typeString.indexOf(')'));
            primitiveType.setVarcharLength(Integer.parseInt(varcharLength.trim()));
        } else {
            switch (typeString) {
            case "LARGEINT":
                primitiveType = DorisDataType.INT;
                size = 16;
                break;
            case "BIGINT":
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
            // case "HLL":
            // primitiveType = DorisDataType.HLL;
            // break;
            // case "BITMAP":
            // primitiveType = DorisDataType.BITMAP;
            // break;
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

        public List<DorisColumn> getRandomNonEmptyInsertColumns() {
            List<DorisColumn> columns = getColumns();
            List<DorisColumn> retColumns = new ArrayList<>();
            List<DorisColumn> remainColumns = new ArrayList<>();
            for (DorisColumn column : columns) {
                if (!column.hasDefaultValue() && !column.isNullable) {
                    retColumns.add(column);
                } else {
                    remainColumns.add(column);
                }
            }
            if (retColumns.isEmpty()) {
                retColumns.addAll(Randomly.nonEmptySubset(remainColumns));
            } else {
                retColumns.addAll(Randomly.subset(remainColumns));
            }
            return retColumns;
        }

    }

    public static DorisSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<DorisTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue;
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
                    String isNullString = rs.getString("Null");
                    assert isNullString.contentEquals("Yes") || isNullString.contentEquals("No");
                    boolean isNullable = isNullString.contentEquals("Yes");
                    String isKeyString = rs.getString("Key");
                    assert isKeyString.contentEquals("true") || isKeyString.contentEquals("false");
                    boolean isKey = isKeyString.contentEquals("true");
                    String defaultValue = rs.getString("Default");
                    boolean hasDefaultValue = defaultValue != null;
                    DorisColumn c = new DorisColumn(columnName, getColumnType(dataType), isKey, isNullable,
                            DorisColumnAggrType.NULL, hasDefaultValue, defaultValue);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
