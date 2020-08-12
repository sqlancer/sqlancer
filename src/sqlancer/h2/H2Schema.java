package sqlancer.h2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.h2.H2Schema.H2Table;

public class H2Schema extends AbstractSchema<H2Table> {

    public enum H2DataType {

        INT, BOOL;

        public static H2DataType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static class H2CompositeDataType {

        private final H2DataType dataType;

        public H2CompositeDataType(H2DataType dataType) {
            this.dataType = dataType;
        }

        public H2DataType getPrimitiveDataType() {
            return dataType;
        }

        public static H2CompositeDataType getRandom() {
            return new H2CompositeDataType(H2DataType.getRandom());
        }

        @Override
        public String toString() {
            return dataType.toString();
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

    public static class H2Table extends AbstractTable<H2Column, TableIndex> {

        public H2Table(String tableName, List<H2Column> columns) {
            super(tableName, columns, Collections.emptyList(), false);
        }

    }

    public static H2Schema fromConnection(Connection con, String databaseName) throws SQLException {
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

    private static List<String> getTableNames(Connection con) throws SQLException {
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

    private static List<H2Column> getTableColumns(Connection con, String tableName) throws SQLException {
        List<H2Column> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SHOW COLUMNS FROM %s;", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String columnType = rs.getString("TYPE");
                    H2DataType primitiveType = getColumnType(columnType);
                    H2Column c = new H2Column(columnName, new H2CompositeDataType(primitiveType));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    private static H2DataType getColumnType(String columnType) {
        if (columnType.startsWith("INTEGER")) {
            return H2DataType.INT;
        } else if (columnType.startsWith("BOOLEAN")) {
            return H2DataType.BOOL;
        } else {
            throw new AssertionError(columnType);
        }
    }

}
