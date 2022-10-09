package sqlancer.yugabyte.ysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PSQLException;

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
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;

public class YSQLSchema extends AbstractSchema<YSQLGlobalState, YSQLTable> {

    private final String databaseName;

    public YSQLSchema(List<YSQLTable> databaseTables, String databaseName) {
        super(databaseTables);
        this.databaseName = databaseName;
    }

    public static YSQLDataType getColumnType(String typeString) {
        switch (typeString) {
        case "smallint":
        case "integer":
        case "bigint":
            return YSQLDataType.INT;
        case "boolean":
            return YSQLDataType.BOOLEAN;
        case "text":
        case "character":
        case "character varying":
        case "name":
            return YSQLDataType.TEXT;
        case "numeric":
            return YSQLDataType.DECIMAL;
        case "double precision":
            return YSQLDataType.FLOAT;
        case "real":
            return YSQLDataType.REAL;
        case "int4range":
            return YSQLDataType.RANGE;
        case "money":
            return YSQLDataType.MONEY;
        case "bytea":
            return YSQLDataType.BYTEA;
        case "bit":
        case "bit varying":
            return YSQLDataType.BIT;
        case "inet":
            return YSQLDataType.INET;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static YSQLSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        try {
            List<YSQLTable> databaseTables = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT table_name, table_schema, table_type, is_insertable_into FROM information_schema.tables WHERE table_schema='public' OR table_schema LIKE 'pg_temp_%' ORDER BY table_name;")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String tableTypeSchema = rs.getString("table_schema");
                        boolean isInsertable = rs.getBoolean("is_insertable_into");
                        // TODO: also check insertable
                        // TODO: insert into view?
                        boolean isView = tableName.startsWith("v"); // tableTypeStr.contains("VIEW") ||
                        // tableTypeStr.contains("LOCAL TEMPORARY") &&
                        // !isInsertable;
                        YSQLTable.TableType tableType = getTableType(tableTypeSchema);
                        List<YSQLColumn> databaseColumns = getTableColumns(con, tableName);
                        List<YSQLIndex> indexes = getIndexes(con, tableName);
                        List<YSQLStatisticsObject> statistics = getStatistics(con);
                        YSQLTable t = new YSQLTable(tableName, databaseColumns, indexes, tableType, statistics, isView,
                                isInsertable);
                        for (YSQLColumn c : databaseColumns) {
                            c.setTable(t);
                        }
                        databaseTables.add(t);
                    }
                }
            }
            return new YSQLSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
    }

    protected static List<YSQLStatisticsObject> getStatistics(SQLConnection con) throws SQLException {
        List<YSQLStatisticsObject> statistics = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT stxname FROM pg_statistic_ext ORDER BY stxname;")) {
                while (rs.next()) {
                    statistics.add(new YSQLStatisticsObject(rs.getString("stxname")));
                }
            }
        }
        return statistics;
    }

    protected static YSQLTable.TableType getTableType(String tableTypeStr) throws AssertionError {
        YSQLTable.TableType tableType;
        if (tableTypeStr.contentEquals("public")) {
            tableType = YSQLTable.TableType.STANDARD;
        } else if (tableTypeStr.startsWith("pg_temp")) {
            tableType = YSQLTable.TableType.TEMPORARY;
        } else {
            throw new AssertionError(tableTypeStr);
        }
        return tableType;
    }

    protected static List<YSQLIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<YSQLIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String
                    .format("SELECT indexname FROM pg_indexes WHERE tablename='%s' ORDER BY indexname;", tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("indexname");
                    if (DBMSCommon.matchesIndexName(indexName)) {
                        indexes.add(YSQLIndex.create(indexName));
                    }
                }
            }
        }
        return indexes;
    }

    protected static List<YSQLColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<YSQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '"
                            + tableName + "' ORDER BY column_name")) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    YSQLColumn c = new YSQLColumn(columnName, getColumnType(dataType));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public YSQLTables getRandomTableNonEmptyTables() {
        return new YSQLTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public enum YSQLDataType {
        // TODO: 23.02.2022 Planned types
        // SMALLINT, INT, BIGINT, NUMERIC, DECIMAL, REAL, DOUBLE_PRECISION, VARCHAR, CHAR, TEXT, DATE, TIME,
        // TIMESTAMP, TIMESTAMPZ, INTERVAL, INTEGER_ARR
        INT, BOOLEAN, BYTEA, TEXT, DECIMAL, FLOAT, REAL, RANGE, MONEY, BIT, INET;

        public static YSQLDataType getRandomType() {
            List<YSQLDataType> dataTypes = new ArrayList<>(Arrays.asList(values()));
            if (YSQLProvider.generateOnlyKnown) {
                dataTypes.remove(YSQLDataType.DECIMAL);
                dataTypes.remove(YSQLDataType.FLOAT);
                dataTypes.remove(YSQLDataType.REAL);
                dataTypes.remove(YSQLDataType.INET);
                dataTypes.remove(YSQLDataType.RANGE);
                dataTypes.remove(YSQLDataType.MONEY);
                dataTypes.remove(YSQLDataType.BIT);
            }
            return Randomly.fromList(dataTypes);
        }
    }

    public static class YSQLColumn extends AbstractTableColumn<YSQLTable, YSQLDataType> {

        public YSQLColumn(String name, YSQLDataType columnType) {
            super(name, null, columnType);
        }

        public static YSQLColumn createDummy(String name) {
            return new YSQLColumn(name, YSQLDataType.INT);
        }

    }

    public static class YSQLTables extends AbstractTables<YSQLTable, YSQLColumn> {

        public YSQLTables(List<YSQLTable> tables) {
            super(tables);
        }

        public YSQLRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<YSQLColumn, YSQLConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    YSQLColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    YSQLConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = YSQLConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                        case INT:
                            constant = YSQLConstant.createIntConstant(randomRowValues.getLong(columnIndex));
                            break;
                        case BOOLEAN:
                            constant = YSQLConstant.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                            break;
                        case TEXT:
                            constant = YSQLConstant.createTextConstant(randomRowValues.getString(columnIndex));
                            break;
                        default:
                            throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new YSQLRowValue(this, values);
            } catch (PSQLException e) {
                throw new IgnoreMeException();
            }

        }

    }

    public static class YSQLRowValue extends AbstractRowValue<YSQLTables, YSQLColumn, YSQLConstant> {

        protected YSQLRowValue(YSQLTables tables, Map<YSQLColumn, YSQLConstant> values) {
            super(tables, values);
        }

    }

    public static class YSQLTable extends AbstractRelationalTable<YSQLColumn, YSQLIndex, YSQLGlobalState> {

        private final TableType tableType;
        private final List<YSQLStatisticsObject> statistics;
        private final boolean isInsertable;

        public YSQLTable(String tableName, List<YSQLColumn> columns, List<YSQLIndex> indexes, TableType tableType,
                List<YSQLStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
        }

        public List<YSQLStatisticsObject> getStatistics() {
            return statistics;
        }

        public TableType getTableType() {
            return tableType;
        }

        public boolean isInsertable() {
            return isInsertable;
        }

        public enum TableType {
            STANDARD, TEMPORARY
        }

    }

    public static final class YSQLStatisticsObject {
        private final String name;

        public YSQLStatisticsObject(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static final class YSQLIndex extends TableIndex {

        private YSQLIndex(String indexName) {
            super(indexName);
        }

        public static YSQLIndex create(String indexName) {
            return new YSQLIndex(indexName);
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

}
