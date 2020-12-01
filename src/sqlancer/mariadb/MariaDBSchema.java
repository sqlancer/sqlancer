package sqlancer.mariadb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable.MariaDBEngine;

public class MariaDBSchema extends AbstractSchema<MariaDBGlobalState, MariaDBTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum MariaDBDataType {
        INT, VARCHAR, REAL, BOOLEAN;
    }

    public static class MariaDBColumn extends AbstractTableColumn<MariaDBTable, MariaDBDataType> {

        private final boolean isPrimaryKey;
        private final int precision;

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public MariaDBColumn(String name, MariaDBDataType columnType, boolean isPrimaryKey, int precision) {
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

    public static class MariaDBTables {
        private final List<MariaDBTable> tables;
        private final List<MariaDBColumn> columns;

        public MariaDBTables(List<MariaDBTable> tables) {
            this.tables = tables;
            columns = new ArrayList<>();
            for (MariaDBTable t : tables) {
                columns.addAll(t.getColumns());
            }
        }

        public String tableNamesAsString() {
            return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
        }

        public List<MariaDBTable> getTables() {
            return tables;
        }

        public List<MariaDBColumn> getColumns() {
            return columns;
        }

        public String columnNamesAsString() {
            return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
                    .collect(Collectors.joining(", "));
        }

        public String columnNamesAsString(Function<MariaDBColumn, String> function) {
            return getColumns().stream().map(function).collect(Collectors.joining(", "));
        }
    }

    private static MariaDBDataType getColumnType(String typeString) {
        switch (typeString) {
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "int":
        case "bigint":
            return MariaDBDataType.INT;
        case "varchar":
        case "tinytext":
        case "mediumtext":
        case "text":
        case "longtext":
        case "char":
            return MariaDBDataType.VARCHAR;
        case "real":
        case "double":
            return MariaDBDataType.REAL;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class MariaDBTable extends AbstractRelationalTable<MariaDBColumn, MariaDBIndex, MariaDBGlobalState> {

        public enum MariaDBEngine {

            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), ARIA("Aria");

            private String s;

            MariaDBEngine(String s) {
                this.s = s;
            }

            public String getTextRepresentation() {
                return s;
            }

            public static MariaDBEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().get();
            }

            public static MariaDBEngine getRandomEngine() {
                return Randomly.fromOptions(MariaDBEngine.values());
            }

        }

        private final MariaDBEngine engine;

        public MariaDBTable(String tableName, List<MariaDBColumn> columns, List<MariaDBIndex> indexes,
                MariaDBEngine engine) {
            super(tableName, columns, indexes, false);
            this.engine = engine;
        }

        public MariaDBEngine getEngine() {
            return engine;
        }

    }

    public static final class MariaDBIndex extends TableIndex {

        private MariaDBIndex(String indexName) {
            super(indexName);
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

    public static MariaDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.MariaDB.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<MariaDBTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            String tableEngineStr = rs.getString("ENGINE");
                            MariaDBEngine engine = MariaDBEngine.get(tableEngineStr);
                            List<MariaDBColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<MariaDBIndex> indexes = getIndexes(con, tableName, databaseName);
                            MariaDBTable t = new MariaDBTable(tableName, databaseColumns, indexes, engine);
                            for (MariaDBColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                return new MariaDBSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<MariaDBIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MariaDBIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(new MariaDBIndex(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<MariaDBColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MariaDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    MariaDBColumn c = new MariaDBColumn(columnName, getColumnType(dataType), isPrimaryKey, precision);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public MariaDBSchema(List<MariaDBTable> databaseTables) {
        super(databaseTables);
    }

}
