package sqlancer.h2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Table;

public class H2Schema extends AbstractSchema<H2GlobalState, H2Table> {

    public enum H2DataType {

        INT, BOOL, VARCHAR, DOUBLE, BINARY;

        public static H2DataType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static class H2CompositeDataType {

        static final int NO_PRECISION = -1;

        private final H2DataType dataType;
        private final int size;
        private final int precision;

        public H2CompositeDataType(H2DataType dataType, int size, int precision) {
            this.dataType = dataType;
            this.size = size;
            this.precision = precision;
        }

        public H2DataType getPrimitiveDataType() {
            return dataType;
        }

        public static H2CompositeDataType getRandom() {
            H2DataType primitiveType = Randomly.fromOptions(H2DataType.INT, H2DataType.BOOL, H2DataType.DOUBLE,
                    H2DataType.BINARY);
            int size = -1;
            int precision = NO_PRECISION;
            switch (primitiveType) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case DOUBLE:
                size = Randomly.fromOptions(4, 8);
                if (Randomly.getBoolean()) {
                    if (size == 4) {
                        precision = (int) Randomly.getNotCachedInteger(1, 25); // TODO: documentation states 0 as lower
                                                                               // bound
                    } else {
                        precision = (int) Randomly.getNotCachedInteger(25, 54);
                    }
                }
                break;
            case VARCHAR:
            case BINARY:
                precision = (int) Randomly.getNotCachedInteger(0, Integer.MAX_VALUE);
                break;
            default:
                break;
            }
            return new H2CompositeDataType(primitiveType, size, precision);
        }

        @Override
        public String toString() {
            switch (dataType) {
            case INT:
                switch (size) {
                case 1:
                    return "TINYINT";
                case 2:
                    return Randomly.fromOptions("SMALLINT", "INT2");
                case 4:
                    return Randomly.fromOptions("INT", "INTEGER", "MEDIUMINT", "INT4", "SIGNED");
                case 8:
                    return Randomly.fromOptions("BIGINT", "INT8");
                default:
                    throw new AssertionError(size);
                }
            case DOUBLE:
                switch (size) {
                case 4:
                    if (precision == NO_PRECISION) {
                        return Randomly.fromOptions("REAL", "FLOAT4");
                    } else {
                        assert precision >= 0 && precision <= 24;
                        return String.format("FLOAT(%d)", precision);
                    }
                case 8:
                    if (precision == NO_PRECISION) {
                        return Randomly.fromOptions("DOUBLE", "DOUBLE PRECISION", "FLOAT8", "FLOAT");
                    } else {
                        assert precision >= 25 && precision <= 53;
                        return String.format("FLOAT(%d)", precision);
                    }
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return /* String varCharType = */ Randomly.fromOptions("VARCHAR", "VARCHAR_IGNORECASE");
            // if (precision == NO_PRECISION) {
            // return varCharType;
            // } else {
            // return String.format("%s(%d)", varCharType, precision);
            // }
            case BINARY:
                return "BINARY";
            // return String.format("BINARY(%d)", precision);
            default:
                return dataType.toString();
            }
        }

    }

    public static class H2Column extends AbstractTableColumn<H2Table, H2CompositeDataType> {

        public H2Column(String name, H2CompositeDataType columnType) {
            super(name, null, columnType);
        }

    }

    public static class H2Tables extends AbstractTables<H2Table, H2Column> {

        public H2Tables(List<H2Table> tables) {
            super(tables);
        }

    }

    public H2Schema(List<H2Table> databaseTables) {
        super(databaseTables);
    }

    public H2Tables getRandomTableNonEmptyTables() {
        return new H2Tables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public static class H2Table extends AbstractRelationalTable<H2Column, TableIndex, H2GlobalState> {

        public H2Table(String tableName, List<H2Column> columns) {
            super(tableName, columns, Collections.emptyList(), tableName.startsWith("V"));
        }

    }

    public static H2Schema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<H2Table> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<H2Column> databaseColumns = getTableColumns(con, tableName);
            H2Table t = new H2Table(tableName, databaseColumns);
            for (H2Column c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new H2Schema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("TABLE_NAME"));
                }
            }
        }
        return tableNames;
    }

    private static List<H2Column> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<H2Column> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SHOW COLUMNS FROM %s;", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String columnType = rs.getString("TYPE");
                    H2DataType primitiveType = getColumnType(columnType);
                    H2Column c = new H2Column(columnName,
                            new H2CompositeDataType(primitiveType, -1, -1 /* TODO: read size and precision */));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    private static H2DataType getColumnType(String columnType) {
        if (columnType.startsWith("INTEGER") || columnType.startsWith("SMALLINT") || columnType.startsWith("TINYINT")
                || columnType.startsWith("BIGINT")) {
            return H2DataType.INT;
        } else if (columnType.startsWith("BOOLEAN")) {
            return H2DataType.BOOL;
        } else if (columnType.startsWith("CHARACTER VARYING")) {
            return H2DataType.VARCHAR;
        } else if (columnType.startsWith("DOUBLE") || columnType.startsWith("DECFLOAT") || columnType.startsWith("REAL")
                || columnType.startsWith("FLOAT")) {
            return H2DataType.DOUBLE;
        } else if (columnType.startsWith("NUMERIC")) {
            return H2DataType.INT;
        } else if (columnType.contentEquals("NULL")) {
            return H2DataType.INT; // for a NULL view column
        } else if (columnType.startsWith("BINARY")) {
            return H2DataType.BINARY;
        } else {
            throw new AssertionError(columnType);
        }
    }

}
