package sqlancer.databend;

import static sqlancer.databend.DatabendSchema.DatabendDataType.INT;

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
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.ast.DatabendConstant;

public class DatabendSchema extends AbstractSchema<DatabendGlobalState, DatabendTable> {

    public enum DatabendDataType {

        INT, VARCHAR, BOOLEAN, FLOAT, NULL;
        // , DATE, TIMESTAMP

        public static DatabendDataType getRandomWithoutNull() {
            DatabendDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == DatabendDataType.NULL);
            return dt;
        }

        public static DatabendDataType getRandomWithoutNullAndVarchar() {
            DatabendDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == DatabendDataType.NULL || dt == DatabendDataType.VARCHAR);
            return dt;
        }

    }

    public static class DatabendCompositeDataType {

        private final DatabendDataType dataType;

        private final int size;

        public DatabendCompositeDataType(DatabendDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public DatabendDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static DatabendCompositeDataType getRandomWithoutNull() {
            DatabendDataType type = DatabendDataType.getRandomWithoutNull();
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
                // case DATE:
                // case TIMESTAMP:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new DatabendCompositeDataType(type, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("BIGINT", "INT64");
                case 4:
                    return Randomly.fromOptions("INT", "INT32");
                case 2:
                    return Randomly.fromOptions("SMALLINT", "INT16");
                case 1:
                    return Randomly.fromOptions("TINYINT", "INT8");
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return Randomly.fromOptions("VARCHAR");
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
                return Randomly.fromOptions("BOOLEAN", "BOOL");
            // case TIMESTAMP:
            // return Randomly.fromOptions("TIMESTAMP", "DATETIME");
            // case DATE:
            // return Randomly.fromOptions("DATE");
            case NULL:
                return Randomly.fromOptions("NULL");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class DatabendColumn extends AbstractTableColumn<DatabendTable, DatabendCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public DatabendColumn(String name, DatabendCompositeDataType columnType, boolean isPrimaryKey,
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

    public static class DatabendTables extends AbstractTables<DatabendTable, DatabendColumn> {

        public DatabendTables(List<DatabendTable> tables) {
            super(tables);
        }

        public DatabendRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String rowValueQuery = String.format("SELECT %s FROM %s ORDER BY 1 LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    tableNamesAsString());
            Map<DatabendColumn, DatabendConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet rs = s.executeQuery(rowValueQuery);
                if (!rs.next()) {
                    throw new IgnoreMeException();
                    // throw new AssertionError("could not find random row " + rowValueQuery + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    DatabendColumn column = getColumns().get(i);
                    int columnIndex = rs.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    DatabendConstant constant;
                    if (rs.getString(columnIndex) == null) {
                        constant = DatabendConstant.createNullConstant();
                    } else {
                        switch (column.getType().getPrimitiveDataType()) {
                        case INT:
                            constant = DatabendConstant.createIntConstant(rs.getLong(columnIndex));
                            break;
                        case BOOLEAN:
                            constant = DatabendConstant.createBooleanConstant(rs.getBoolean(columnIndex));
                            break;
                        case VARCHAR:
                            constant = DatabendConstant.createStringConstant(rs.getString(columnIndex));
                            break;
                        default:
                            throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                assert !rs.next();
                return new DatabendRowValue(this, values);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        }

    }

    public static class DatabendRowValue extends AbstractRowValue<DatabendTables, DatabendColumn, DatabendConstant> {

        DatabendRowValue(DatabendTables tables, Map<DatabendColumn, DatabendConstant> values) {
            super(tables, values);
        }

    }

    public DatabendSchema(List<DatabendTable> databaseTables) {
        super(databaseTables);
    }

    public DatabendTables getRandomTableNonEmptyTables() {
        return new DatabendTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public DatabendTables getRandomTableNonEmptyAndViewTables() {
        List<DatabendTable> tables = getDatabaseTables().stream().filter(t -> !t.isView()).collect(Collectors.toList());
        tables = Randomly.nonEmptySubset(tables);
        return new DatabendTables(tables);
    }

    private static DatabendCompositeDataType getColumnType(String typeString) {
        DatabendDataType primitiveType;
        int size = -1;
        if (typeString.startsWith("DECIMAL")) { // Ugly hack
            return new DatabendCompositeDataType(DatabendDataType.FLOAT, 8);
        }
        switch (typeString) {
        case "INT":
            primitiveType = INT;
            size = 4;
            break;
        case "SMALLINT":
            primitiveType = INT;
            size = 2;
            break;
        case "BIGINT":
            primitiveType = INT;
            size = 8;
            break;
        case "TINYINT":
            primitiveType = INT;
            size = 1;
            break;
        case "VARCHAR":
            primitiveType = DatabendDataType.VARCHAR;
            break;
        case "FLOAT":
            primitiveType = DatabendDataType.FLOAT;
            size = 4;
            break;
        case "DOUBLE":
            primitiveType = DatabendDataType.FLOAT;
            size = 8;
            break;
        case "BOOLEAN":
            primitiveType = DatabendDataType.BOOLEAN;
            break;
        // case "DATE":
        // primitiveType = DatabendDataType.DATE;
        // break;
        // case "TIMESTAMP":
        // primitiveType = DatabendDataType.TIMESTAMP;
        // break;
        case "NULL":
            primitiveType = DatabendDataType.NULL;
            break;
        case "INTERVAL":
            throw new IgnoreMeException();
        // TODO: caused when a view contains a computation like ((TIMESTAMP '1970-01-05 11:26:57')-(TIMESTAMP
        // '1969-12-29 06:50:27'))
        default:
            throw new AssertionError(typeString);
        }
        return new DatabendCompositeDataType(primitiveType, size);
    }

    public static class DatabendTable extends AbstractRelationalTable<DatabendColumn, TableIndex, DatabendGlobalState> {

        public DatabendTable(String tableName, List<DatabendColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static DatabendSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<DatabendTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con, databaseName);
        for (String tableName : tableNames) {
            List<DatabendColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
            boolean isView = tableName.startsWith("v");
            DatabendTable t = new DatabendTable(tableName, databaseColumns, isView);
            for (DatabendColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new DatabendSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con, String databaseName) throws SQLException {
        List<String> tableNames = null;
        tableNames = new ArrayList<>();

        final String sqlStatement = String.format(
                "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '%s' and table_type='BASE TABLE' ",
                databaseName);
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(sqlStatement)) {
                while (rs.next()) {
                    tableNames.add(rs.getString("table_name"));
                }
            }
        }
        return tableNames;
    }

    private static List<DatabendColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<DatabendColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s' and table_name ='%s'",
                    databaseName, tableName))) {
                try { // 没有catch的话rs.next()会报SQLException：Not a navigable ResultSet
                    while (rs.next()) {
                        String columnName = rs.getString("column_name");
                        String dataType = rs.getString("data_type");
                        boolean isNullable = rs.getBoolean("is_nullable");
                        // boolean isPrimaryKey = rs.getString("pk").contains("true");
                        boolean isPrimaryKey = false; // 没找到主键元数据
                        DatabendColumn c = new DatabendColumn(columnName, getColumnType(dataType), isPrimaryKey,
                                isNullable);
                        columns.add(c);
                    }
                } catch (Exception e) {
                    System.out.println("TableColumns->SQLException：Not a navigable ResultSet");
                }
            }
        }

        return columns;
    }

}
