package sqlancer.yugabyte.ysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import org.postgresql.util.PSQLException;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.*;
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
            case "name":
                return YSQLDataType.TEXT;
            case "numeric":
                return YSQLDataType.NUMERIC;
            case "decimal":
                return YSQLDataType.DECIMAL;
            case "double precision":
                return YSQLDataType.DOUBLE_PRECISION;
            case "real":
                return YSQLDataType.REAL;
            case "int4range":
                return YSQLDataType.INT4RANGE;
            case "int8range":
                return YSQLDataType.INT8RANGE;
            case "numrange":
                return YSQLDataType.NUMRANGE;
            case "tsrange":
                return YSQLDataType.TSRANGE;
            case "tstzrange":
                return YSQLDataType.TSTZRANGE;
            case "daterange":
                return YSQLDataType.DATERANGE;
            case "money":
                return YSQLDataType.MONEY;
            case "bytea":
                return YSQLDataType.BYTEA;
            case "bit":
            case "bit varying":
                return YSQLDataType.BIT;
            case "inet":
                return YSQLDataType.INET;
            case "cidr":
                return YSQLDataType.CIDR;
            case "macaddr":
                return YSQLDataType.MACADDR;
            case "date":
                return YSQLDataType.DATE;
            case "time":
            case "time without time zone":
                return YSQLDataType.TIME;
            case "timestamp":
            case "timestamp without time zone":
                return YSQLDataType.TIMESTAMP;
            case "timestamp with time zone":
                return YSQLDataType.TIMESTAMPTZ;
            case "interval":
                return YSQLDataType.INTERVAL;
            case "uuid":
                return YSQLDataType.UUID;
            case "json":
                return YSQLDataType.JSON;
            case "jsonb":
                return YSQLDataType.JSONB;
            case "point":
                return YSQLDataType.POINT;
            case "line":
                return YSQLDataType.LINE;
            case "lseg":
                return YSQLDataType.LSEG;
            case "box":
                return YSQLDataType.BOX;
            case "path":
                return YSQLDataType.PATH;
            case "polygon":
                return YSQLDataType.POLYGON;
            case "circle":
                return YSQLDataType.CIRCLE;
            case "character varying":
            case "varchar":
                return YSQLDataType.VARCHAR;
            case "character":
            case "char":
                return YSQLDataType.CHAR;
            case "ARRAY":
                // PostgreSQL array types are reported as "ARRAY" in information_schema
                // We'll map to INT_ARRAY as a default for now
                return YSQLDataType.INT_ARRAY;
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
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "select relname from pg_class where relkind = 'm';")) {
                    while (rs.next()) {
                        String tableName = rs.getString("relname");
                        boolean isInsertable = false;
                        List<YSQLColumn> databaseColumns = getTableColumns(con, tableName);
                        List<YSQLStatisticsObject> statistics = getStatistics(con);
                        YSQLTable t = new YSQLTable(tableName, databaseColumns, new ArrayList<>(), YSQLTable.TableType.MATERIALIZED_VIEW, statistics, false,
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

    public boolean getDatabaseIsColocated(SQLConnection con) {
        try (Statement s = con.createStatement(); ResultSet rs = s.executeQuery("SELECT yb_is_database_colocated();")) {
            rs.next();
            String result = rs.getString(1);
            // The query will result in a 'f' for a non-colocated database
            return !"f".equals(result);

        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    public YSQLTables getRandomTableNonEmptyTables() {
        return new YSQLTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public List<YSQLTable> getRandomMaterializedView() {
        return getDatabaseTables().stream().filter(YSQLTable::isMaterializedView).collect(Collectors.toList());
    }

    public enum YSQLDataType {
        // Basic numeric types
        SMALLINT, INT, BIGINT, NUMERIC, DECIMAL, REAL, DOUBLE_PRECISION, 
        // Character types
        VARCHAR, CHAR, TEXT, 
        // Binary types
        BYTEA, BIT,
        // Date/Time types
        DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL,
        // Boolean
        BOOLEAN,
        // Network types
        INET, CIDR, MACADDR,
        // Geometric types
        POINT, LINE, LSEG, BOX, PATH, POLYGON, CIRCLE,
        // UUID
        UUID,
        // JSON types
        JSON, JSONB,
        // Range types
        INT4RANGE, INT8RANGE, NUMRANGE, TSRANGE, TSTZRANGE, DATERANGE,
        // Array types (PostgreSQL 15 feature)
        INT_ARRAY, TEXT_ARRAY, BOOLEAN_ARRAY,
        // Money type
        MONEY,
        // Legacy types kept for compatibility
        FLOAT, RANGE;

        public static YSQLDataType getRandomType() {
            List<YSQLDataType> dataTypes = new ArrayList<>(Arrays.asList(values()));
            if (YSQLProvider.generateOnlyKnown) {
                // Remove types that might not be fully supported in test oracles
                dataTypes.remove(YSQLDataType.DECIMAL);
                dataTypes.remove(YSQLDataType.FLOAT);
                dataTypes.remove(YSQLDataType.REAL);
                dataTypes.remove(YSQLDataType.DOUBLE_PRECISION);
                dataTypes.remove(YSQLDataType.NUMERIC);
                
                // Remove network types
                dataTypes.remove(YSQLDataType.INET);
                dataTypes.remove(YSQLDataType.CIDR);
                dataTypes.remove(YSQLDataType.MACADDR);
                
                // Remove geometric types
                dataTypes.remove(YSQLDataType.POINT);
                dataTypes.remove(YSQLDataType.LINE);
                dataTypes.remove(YSQLDataType.LSEG);
                dataTypes.remove(YSQLDataType.BOX);
                dataTypes.remove(YSQLDataType.PATH);
                dataTypes.remove(YSQLDataType.POLYGON);
                dataTypes.remove(YSQLDataType.CIRCLE);
                
                // Remove range types (YugabyteDB has limited support)
                dataTypes.remove(YSQLDataType.RANGE);
                dataTypes.remove(YSQLDataType.INT4RANGE);
                dataTypes.remove(YSQLDataType.INT8RANGE);
                dataTypes.remove(YSQLDataType.NUMRANGE);
                dataTypes.remove(YSQLDataType.TSRANGE);
                dataTypes.remove(YSQLDataType.TSTZRANGE);
                dataTypes.remove(YSQLDataType.DATERANGE);
                
                // Remove other complex types
                dataTypes.remove(YSQLDataType.MONEY);
                dataTypes.remove(YSQLDataType.BIT);
                dataTypes.remove(YSQLDataType.UUID);
                dataTypes.remove(YSQLDataType.JSON);
                dataTypes.remove(YSQLDataType.JSONB);
                
                // Remove array types for now
                dataTypes.remove(YSQLDataType.INT_ARRAY);
                dataTypes.remove(YSQLDataType.TEXT_ARRAY);
                dataTypes.remove(YSQLDataType.BOOLEAN_ARRAY);
                
                // Remove date/time types that need special handling
                dataTypes.remove(YSQLDataType.TIME);
                dataTypes.remove(YSQLDataType.INTERVAL);
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
                            case SMALLINT:
                            case INT:
                            case BIGINT:
                                constant = YSQLConstant.createIntConstant(randomRowValues.getLong(columnIndex));
                                break;
                            case BOOLEAN:
                                constant = YSQLConstant.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                                break;
                            case VARCHAR:
                            case CHAR:
                            case TEXT:
                                constant = YSQLConstant.createTextConstant(randomRowValues.getString(columnIndex));
                                break;
                            case NUMERIC:
                            case DECIMAL:
                                constant = YSQLConstant.createDecimalConstant(randomRowValues.getBigDecimal(columnIndex));
                                break;
                            case REAL:
                            case FLOAT:
                                constant = YSQLConstant.createFloatConstant(randomRowValues.getFloat(columnIndex));
                                break;
                            case DOUBLE_PRECISION:
                                constant = YSQLConstant.createDoubleConstant(randomRowValues.getDouble(columnIndex));
                                break;
                            case DATE:
                            case TIMESTAMP:
                            case TIMESTAMPTZ:
                                // For now, treat as text
                                constant = YSQLConstant.createTextConstant(randomRowValues.getString(columnIndex));
                                break;
                            case BYTEA:
                                constant = YSQLConstant.createByteConstant(randomRowValues.getString(columnIndex));
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

        public boolean isMaterializedView() {
            return tableType == YSQLTable.TableType.MATERIALIZED_VIEW;
        }

        public enum TableType {
            STANDARD, TEMPORARY, MATERIALIZED_VIEW
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
