package sqlancer.mariadb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable.MariaDBEngine;

public class MariaDBSchema {

    private static final int NR_SCHEMA_READ_TRIES = 10;
    private final List<MariaDBTable> databaseTables;

    public enum MariaDBDataType {
        INT, VARCHAR, REAL, BOOLEAN;
    }

    public static class MariaDBColumn implements Comparable<MariaDBColumn> {

        private final String name;
        private final MariaDBDataType columnType;
        private final boolean isPrimaryKey;
        private MariaDBTable table;
        private final int precision;

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public MariaDBColumn(String name, MariaDBDataType columnType, boolean isPrimaryKey, int precision) {
            this.name = name;
            this.columnType = columnType;
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
        }

        @Override
        public String toString() {
            return String.format("%s.%s: %s", table.getName(), name, columnType);
        }

        @Override
        public int hashCode() {
            return name.hashCode() + 11 * columnType.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MariaDBColumn)) {
                return false;
            } else {
                MariaDBColumn c = (MariaDBColumn) obj;
                return table.getName().contentEquals(getName()) && name.equals(c.name);
            }
        }

        public String getName() {
            return name;
        }

        public String getFullQualifiedName() {
            return table.getName() + "." + getName();
        }

        public MariaDBDataType getColumnType() {
            return columnType;
        }

        public int getPrecision() {
            return precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public void setTable(MariaDBTable t) {
            this.table = t;
        }

        public MariaDBTable getTable() {
            return table;
        }

        @Override
        public int compareTo(MariaDBColumn o) {
            if (o.getTable().equals(this.getTable())) {
                return name.compareTo(o.getName());
            } else {
                return o.getTable().compareTo(table);
            }
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

    public static class MariaDBTable implements Comparable<MariaDBTable> {

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

        private final String tableName;
        private final List<MariaDBColumn> columns;
        private final List<MariaDBIndex> indexes;
        private final MariaDBEngine engine;

        public MariaDBTable(String tableName, List<MariaDBColumn> columns, List<MariaDBIndex> indexes,
                MariaDBEngine engine) {
            this.tableName = tableName;
            this.indexes = indexes;
            this.engine = engine;
            this.columns = Collections.unmodifiableList(columns);
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(tableName + "\n");
            for (MariaDBColumn c : columns) {
                sb.append("\t" + c + "\n");
            }
            return sb.toString();
        }

        public List<MariaDBIndex> getIndexes() {
            return indexes;
        }

        public String getName() {
            return tableName;
        }

        public List<MariaDBColumn> getColumns() {
            return columns;
        }

        public String getColumnsAsString() {
            return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
        }

        public String getColumnsAsString(Function<MariaDBColumn, String> function) {
            return columns.stream().map(function).collect(Collectors.joining(", "));
        }

        public MariaDBColumn getRandomColumn() {
            return Randomly.fromList(columns);
        }

        public boolean hasIndexes() {
            return !indexes.isEmpty();
        }

        public MariaDBIndex getRandomIndex() {
            return Randomly.fromList(indexes);
        }

        @Override
        public int compareTo(MariaDBTable o) {
            return o.getName().compareTo(tableName);
        }

        public List<MariaDBColumn> getRandomNonEmptyColumnSubset() {
            return Randomly.nonEmptySubset(getColumns());
        }

        public MariaDBEngine getEngine() {
            return engine;
        }

        public boolean hasPrimaryKey() {
            return columns.stream().anyMatch(c -> c.isPrimaryKey());
        }
    }

    public static final class MariaDBIndex {

        private final String indexName;

        private MariaDBIndex(String indexName) {
            this.indexName = indexName;
        }

        public static MariaDBIndex create(String indexName) {
            return new MariaDBIndex(indexName);
        }

        public String getIndexName() {
            if (indexName.contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return indexName;
            }
        }

    }

    public static MariaDBSchema fromConnection(Connection con, String databaseName) throws SQLException {
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

    private static List<MariaDBIndex> getIndexes(Connection con, String tableName, String databaseName)
            throws SQLException {
        List<MariaDBIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(MariaDBIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<MariaDBColumn> getTableColumns(Connection con, String tableName, String databaseName)
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
        this.databaseTables = Collections.unmodifiableList(databaseTables);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (MariaDBTable t : getDatabaseTables()) {
            sb.append(t + "\n");
        }
        return sb.toString();
    }

    public MariaDBTable getRandomTable() {
        return Randomly.fromList(getDatabaseTables());
    }

    public MariaDBTables getRandomTableNonEmptyTables() {
        return new MariaDBTables(Randomly.nonEmptySubset(databaseTables));
    }

    public List<MariaDBTable> getDatabaseTables() {
        return databaseTables;
    }

    public List<MariaDBTable> getDatabaseTablesRandomSubsetNotEmpty() {
        return Randomly.nonEmptySubset(databaseTables);
    }

}
