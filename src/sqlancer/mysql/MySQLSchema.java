package sqlancer.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.StateToReproduce.MySQLStateToReproduce;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTable.MySQLEngine;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.schema.AbstractSchema;
import sqlancer.schema.AbstractTable;
import sqlancer.schema.AbstractTableColumn;
import sqlancer.schema.AbstractTables;
import sqlancer.schema.TableIndex;

public class MySQLSchema extends AbstractSchema<MySQLTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum MySQLDataType {
        INT, VARCHAR, FLOAT, DOUBLE, DECIMAL;

        public static MySQLDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public boolean isNumeric() {
            switch (this) {
            case INT:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return true;
            case VARCHAR:
                return false;
            default:
                throw new AssertionError(this);
            }
        }

    }

    public static class MySQLColumn extends AbstractTableColumn<MySQLTable, MySQLDataType> {

        private final boolean isPrimaryKey;
        private final int precision;

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public MySQLColumn(String name, MySQLDataType columnType, boolean isPrimaryKey, int precision) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

    }

    public static class MySQLTables extends AbstractTables<MySQLTable, MySQLColumn> {

        public MySQLTables(List<MySQLTable> tables) {
            super(tables);
        }

        public MySQLRowValue getRandomRowValue(Connection con, MySQLStateToReproduce state) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<MySQLColumn, MySQLConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n" + state);
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    MySQLColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    // String typeString = randomRowValues.getString(columnIndex + getColumns().size());
                    // MySQLDataType valueType = getColumnType(typeString);
                    MySQLConstant constant;
                    // if (randomRowValues.getString(columnIndex) == null) {
                    // value = null;
                    // constant = MySQLConstant.createNullConstant();
                    // } else {
                    // switch (valueType) {
                    // case INT:
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = MySQLConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                        case INT:
                            value = randomRowValues.getLong(columnIndex);
                            constant = MySQLConstant.createIntConstant((long) value);
                            break;
                        case VARCHAR:
                            value = randomRowValues.getString(columnIndex);
                            constant = MySQLConstant.createStringConstant((String) value);
                            break;
                        default:
                            throw new AssertionError(column.getType());
                        }
                    }
                    // break;
                    // default:
                    // throw new AssertionError(valueType);
                    // }
                    // }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                state.randomRowValues = values;
                return new MySQLRowValue(this, values);
            }

        }

    }

    private static MySQLDataType getColumnType(String typeString) {
        switch (typeString) {
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "int":
        case "bigint":
            return MySQLDataType.INT;
        case "varchar":
        case "tinytext":
        case "mediumtext":
        case "text":
        case "longtext":
            return MySQLDataType.VARCHAR;
        case "double":
            return MySQLDataType.DOUBLE;
        case "float":
            return MySQLDataType.FLOAT;
        case "decimal":
            return MySQLDataType.DECIMAL;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class MySQLRowValue {

        private final MySQLTables tables;
        private final Map<MySQLColumn, MySQLConstant> values;

        MySQLRowValue(MySQLTables tables, Map<MySQLColumn, MySQLConstant> values) {
            this.tables = tables;
            this.values = values;
        }

        public MySQLTables getTable() {
            return tables;
        }

        public Map<MySQLColumn, MySQLConstant> getValues() {
            return values;
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            int i = 0;
            for (MySQLColumn c : tables.getColumns()) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                sb.append(values.get(c));
            }
            return sb.toString();
        }

        public String getRowValuesAsString() {
            List<MySQLColumn> columnsToCheck = tables.getColumns();
            return getRowValuesAsString(columnsToCheck);
        }

        public String getRowValuesAsString(List<MySQLColumn> columnsToCheck) {
            StringBuilder sb = new StringBuilder();
            Map<MySQLColumn, MySQLConstant> expectedValues = getValues();
            for (int i = 0; i < columnsToCheck.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                MySQLConstant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
                MySQLToStringVisitor visitor = new MySQLToStringVisitor();
                visitor.visit(expectedColumnValue);
                sb.append(visitor.get());
            }
            return sb.toString();
        }

    }

    public static class MySQLTable extends AbstractTable<MySQLColumn, MySQLIndex> {

        public enum MySQLEngine {
            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), MEMORY("MEMORY"), HEAP("HEAP"), CSV("CSV"), MERGE("MERGE"),
            ARCHIVE("ARCHIVE"), FEDERATED("FEDERATED");

            private String s;

            MySQLEngine(String s) {
                this.s = s;
            }

            String getTextRepresentation() {
                return s;
            }

            public static MySQLEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().get();
            }

        }

        private final MySQLEngine engine;

        public MySQLTable(String tableName, List<MySQLColumn> columns, List<MySQLIndex> indexes, MySQLEngine engine) {
            super(tableName, columns, indexes, false /* TODO: support views */);
            this.engine = engine;
        }

        public MySQLEngine getEngine() {
            return engine;
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static final class MySQLIndex extends TableIndex {

        private MySQLIndex(String indexName) {
            super(indexName);
        }

        public static MySQLIndex create(String indexName) {
            return new MySQLIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static MySQLSchema fromConnection(Connection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.mysql.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<MySQLTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            String tableEngineStr = rs.getString("ENGINE");
                            MySQLEngine engine = MySQLEngine.get(tableEngineStr);
                            List<MySQLColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<MySQLIndex> indexes = getIndexes(con, tableName, databaseName);
                            MySQLTable t = new MySQLTable(tableName, databaseColumns, indexes, engine);
                            for (MySQLColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                return new MySQLSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<MySQLIndex> getIndexes(Connection con, String tableName, String databaseName)
            throws SQLException {
        List<MySQLIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(MySQLIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<MySQLColumn> getTableColumns(Connection con, String tableName, String databaseName)
            throws SQLException {
        List<MySQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    MySQLColumn c = new MySQLColumn(columnName, getColumnType(dataType), isPrimaryKey, precision);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public MySQLSchema(List<MySQLTable> databaseTables) {
        super(databaseTables);
    }

    public MySQLTables getRandomTableNonEmptyTables() {
        return new MySQLTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
