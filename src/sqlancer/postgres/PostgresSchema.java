package sqlancer.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.PostgresStateToReproduce;
import sqlancer.postgres.PostgresSchema.PostgresTable.TableType;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.schema.AbstractTable;
import sqlancer.schema.AbstractTableColumn;
import sqlancer.schema.AbstractTables;
import sqlancer.schema.TableIndex;

public class PostgresSchema {

    private final List<PostgresTable> databaseTables;
    private final String databaseName;

    public enum PostgresDataType {
        INT, BOOLEAN, TEXT, DECIMAL, FLOAT, REAL, RANGE, MONEY, BIT, INET;

        public static PostgresDataType getRandomType() {
            List<PostgresDataType> dataTypes = Arrays.asList(values());
            if (PostgresProvider.generateOnlyKnown) {
                dataTypes.remove(PostgresDataType.DECIMAL);
                dataTypes.remove(PostgresDataType.FLOAT);
                dataTypes.remove(PostgresDataType.REAL);
                dataTypes.remove(PostgresDataType.INET);
            }
            return Randomly.fromList(dataTypes);
        }
    }

    public static class PostgresColumn extends AbstractTableColumn<PostgresTable, PostgresDataType> {

        public PostgresColumn(String name, PostgresDataType columnType) {
            super(name, null, columnType);
        }

        public static PostgresColumn createDummy(String name) {
            return new PostgresColumn(name, PostgresDataType.INT);
        }

    }

    public static class PostgresTables extends AbstractTables<PostgresTable, PostgresColumn> {

        public PostgresTables(List<PostgresTable> tables) {
            super(tables);
        }

        public PostgresRowValue getRandomRowValue(Connection con, PostgresStateToReproduce state) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<PostgresColumn, PostgresConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n" + state);
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    PostgresColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    PostgresConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = PostgresConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                        case INT:
                            constant = PostgresConstant.createIntConstant(randomRowValues.getLong(columnIndex));
                            break;
                        case BOOLEAN:
                            constant = PostgresConstant.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                            break;
                        case TEXT:
                            constant = PostgresConstant.createTextConstant(randomRowValues.getString(columnIndex));
                            break;
                        default:
                            throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                state.randomRowValues = values;
                return new PostgresRowValue(this, values);
            }

        }

    }

    private static PostgresDataType getColumnType(String typeString) {
        switch (typeString) {
        case "smallint":
        case "integer":
        case "bigint":
            return PostgresDataType.INT;
        case "boolean":
            return PostgresDataType.BOOLEAN;
        case "text":
        case "character":
        case "character varying":
        case "name":
            return PostgresDataType.TEXT;
        case "numeric":
            return PostgresDataType.DECIMAL;
        case "double precision":
            return PostgresDataType.FLOAT;
        case "real":
            return PostgresDataType.REAL;
        case "int4range":
            return PostgresDataType.RANGE;
        case "money":
            return PostgresDataType.MONEY;
        case "bit":
        case "bit varying":
            return PostgresDataType.BIT;
        case "inet":
            return PostgresDataType.INET;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class PostgresRowValue {

        private final PostgresTables tables;
        private final Map<PostgresColumn, PostgresConstant> values;

        PostgresRowValue(PostgresTables tables, Map<PostgresColumn, PostgresConstant> values) {
            this.tables = tables;
            this.values = values;
        }

        public PostgresTables getTable() {
            return tables;
        }

        public Map<PostgresColumn, PostgresConstant> getValues() {
            return values;
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            int i = 0;
            for (PostgresColumn c : tables.getColumns()) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                sb.append(values.get(c));
            }
            return sb.toString();
        }

        public String getRowValuesAsString() {
            List<PostgresColumn> columnsToCheck = tables.getColumns();
            return getRowValuesAsString(columnsToCheck);
        }

        public String getRowValuesAsString(List<PostgresColumn> columnsToCheck) {
            StringBuilder sb = new StringBuilder();
            Map<PostgresColumn, PostgresConstant> expectedValues = getValues();
            for (int i = 0; i < columnsToCheck.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                PostgresConstant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
                PostgresToStringVisitor visitor = new PostgresToStringVisitor();
                visitor.visit(expectedColumnValue);
                sb.append(visitor.get());
            }
            return sb.toString();
        }

    }

    public static class PostgresTable extends AbstractTable<PostgresColumn, PostgresIndex> {

        public enum TableType {
            STANDARD, TEMPORARY
        }

        private final TableType tableType;
        private final List<PostgresStatisticsObject> statistics;
        private final boolean isInsertable;

        public PostgresTable(String tableName, List<PostgresColumn> columns, List<PostgresIndex> indexes,
                TableType tableType, List<PostgresStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
        }

        public List<PostgresStatisticsObject> getStatistics() {
            return statistics;
        }

        public TableType getTableType() {
            return tableType;
        }

        public boolean isInsertable() {
            return isInsertable;
        }

    }

    public static final class PostgresStatisticsObject {
        private final String name;

        public PostgresStatisticsObject(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static final class PostgresIndex extends TableIndex {

        private PostgresIndex(String indexName) {
            super(indexName);
        }

        public static PostgresIndex create(String indexName) {
            return new PostgresIndex(indexName);
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

    public static PostgresSchema fromConnection(Connection con, String databaseName) throws SQLException {
        Exception ex = null;
        try {
            List<PostgresTable> databaseTables = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT table_name, table_schema, table_type, is_insertable_into FROM information_schema.tables WHERE table_schema='public' OR table_schema LIKE 'pg_temp_%';")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String tableTypeSchema = rs.getString("table_schema");
                        boolean isInsertable = rs.getBoolean("is_insertable_into");
                        // TODO: also check insertable
                        // TODO: insert into view?
                        boolean isView = tableName.startsWith("v"); // tableTypeStr.contains("VIEW") ||
                                                                    // tableTypeStr.contains("LOCAL TEMPORARY") &&
                                                                    // !isInsertable;
                        PostgresTable.TableType tableType = getTableType(tableTypeSchema);
                        List<PostgresColumn> databaseColumns = getTableColumns(con, tableName);
                        List<PostgresIndex> indexes = getIndexes(con, tableName);
                        List<PostgresStatisticsObject> statistics = getStatistics(con);
                        PostgresTable t = new PostgresTable(tableName, databaseColumns, indexes, tableType, statistics,
                                isView, isInsertable);
                        for (PostgresColumn c : databaseColumns) {
                            c.setTable(t);
                        }
                        databaseTables.add(t);
                    }
                }
            }
            return new PostgresSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            ex = e;
        }
        throw new AssertionError(ex);
    }

    private static List<PostgresStatisticsObject> getStatistics(Connection con) throws SQLException {
        List<PostgresStatisticsObject> statistics = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT stxname FROM pg_statistic_ext;")) {
                while (rs.next()) {
                    statistics.add(new PostgresStatisticsObject(rs.getString("stxname")));
                }
            }
        }
        return statistics;
    }

    private static PostgresTable.TableType getTableType(String tableTypeStr) throws AssertionError {
        PostgresTable.TableType tableType;
        if (tableTypeStr.contentEquals("public")) {
            tableType = TableType.STANDARD;
        } else if (tableTypeStr.startsWith("pg_temp")) {
            tableType = TableType.TEMPORARY;
        } else {
            throw new AssertionError(tableTypeStr);
        }
        return tableType;
    }

    private static List<PostgresIndex> getIndexes(Connection con, String tableName) throws SQLException {
        List<PostgresIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery(String.format("SELECT indexname FROM pg_indexes WHERE tablename='%s';", tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("indexname");
                    if (indexName.length() != 2) {
                        // FIXME: implement cleanly
                        continue; // skip internal indexes
                    }
                    indexes.add(PostgresIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<PostgresColumn> getTableColumns(Connection con, String tableName) throws SQLException {
        List<PostgresColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '"
                            + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    PostgresColumn c = new PostgresColumn(columnName, getColumnType(dataType));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public PostgresSchema(List<PostgresTable> databaseTables, String databaseName) {
        this.databaseTables = Collections.unmodifiableList(databaseTables);
        this.databaseName = databaseName;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (PostgresTable t : getDatabaseTables()) {
            sb.append(t);
            sb.append("\n");
        }
        return sb.toString();
    }

    public PostgresTable getRandomTable() {
        return Randomly.fromList(getDatabaseTables());
    }

    public PostgresTables getRandomTableNonEmptyTables() {
        return new PostgresTables(Randomly.nonEmptySubset(databaseTables));
    }

    public List<PostgresTable> getDatabaseTables() {
        return databaseTables;
    }

    public List<PostgresTable> getDatabaseTablesRandomSubsetNotEmpty() {
        return Randomly.nonEmptySubset(databaseTables);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public PostgresTable getRandomTable(Function<PostgresTable, Boolean> f) {
        List<PostgresTable> relevantTables = databaseTables.stream().filter(t -> f.apply(t))
                .collect(Collectors.toList());
        if (relevantTables.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(relevantTables);
    }

}
