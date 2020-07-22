package sqlancer.sqlite3.schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.SQLite3StateToReproduce;
import sqlancer.schema.AbstractTable;
import sqlancer.schema.AbstractTableColumn;
import sqlancer.schema.TableIndex;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3ToStringVisitor;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table.TableKind;

public class SQLite3Schema {

    private final List<SQLite3Table> databaseTables;
    private final List<String> indexNames;

    public List<String> getIndexNames() {
        return indexNames;
    }

    public String getRandomIndexOrBailout() {
        if (indexNames.isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(indexNames);
        }
    }

    public SQLite3Table getRandomTableOrBailout() {
        if (databaseTables.isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(getDatabaseTables());
        }
    }

    public static class SQLite3Column extends AbstractTableColumn<SQLite3Table, SQLite3DataType> {

        private final boolean isInteger; // "INTEGER" type, not "INT"
        private final SQLite3CollateSequence collate;
        boolean generated;
        private final boolean isPrimaryKey;

        public enum SQLite3CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static SQLite3CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public SQLite3Column(String name, SQLite3DataType columnType, boolean isInteger, boolean isPrimaryKey,
                SQLite3CollateSequence collate) {
            super(name, null, columnType);
            this.isInteger = isInteger;
            this.isPrimaryKey = isPrimaryKey;
            this.collate = collate;
            this.generated = false;
            assert !isInteger || columnType == SQLite3DataType.INT;
        }

        public SQLite3Column(String rowId, SQLite3DataType columnType, boolean isInteger,
                SQLite3CollateSequence collate, boolean generated) {
            this(rowId, columnType, isInteger, generated, collate);
            this.generated = generated;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isOnlyPrimaryKey() {
            return isPrimaryKey && getTable().getColumns().stream().filter(c -> c.isPrimaryKey()).count() == 1;
        }

        // see https://www.sqlite.org/lang_createtable.html#rowid
        /**
         * If a table has a single column primary key and the declared type of that column is "INTEGER" and the table is
         * not a WITHOUT ROWID table, then the column is known as an INTEGER PRIMARY KEY.
         */
        public boolean isIntegerPrimaryKey() {
            return isInteger && isOnlyPrimaryKey() && !getTable().hasWithoutRowid();
        }

        public SQLite3CollateSequence getCollateSequence() {
            return collate;
        }

        public boolean isGenerated() {
            return generated;
        }

        public static SQLite3Column createDummy(String name) {
            return new SQLite3Column(name, SQLite3DataType.INT, false, false, null);
        }

    }

    public static SQLite3Constant getConstant(ResultSet randomRowValues, int columnIndex, SQLite3DataType valueType)
            throws SQLException, AssertionError {
        Object value;
        SQLite3Constant constant;
        if (randomRowValues.getString(columnIndex) == null) {
            value = null;
            constant = SQLite3Constant.createNullConstant();
        } else {
            switch (valueType) {
            case INT:
                value = randomRowValues.getLong(columnIndex);
                constant = SQLite3Constant.createIntConstant((long) value);
                break;
            case REAL:
                value = randomRowValues.getDouble(columnIndex);
                constant = SQLite3Constant.createRealConstant((double) value);
                break;
            case TEXT:
            case NONE:
                value = randomRowValues.getString(columnIndex);
                constant = SQLite3Constant.createTextConstant((String) value);
                break;
            case BINARY:
                value = randomRowValues.getBytes(columnIndex);
                constant = SQLite3Constant.createBinaryConstant((byte[]) value);
                break;
            default:
                throw new AssertionError(valueType);
            }
        }
        return constant;
    }

    public static class SQLite3Tables {
        private final List<SQLite3Table> tables;
        private final List<SQLite3Column> columns;

        public SQLite3Tables(List<SQLite3Table> tables) {
            this.tables = tables;
            columns = new ArrayList<>();
            for (SQLite3Table t : tables) {
                columns.addAll(t.getColumns());
            }
        }

        public String tableNamesAsString() {
            return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
        }

        public List<SQLite3Table> getTables() {
            return tables;
        }

        public List<SQLite3Column> getColumns() {
            return columns;
        }

        public String columnNamesAsString() {
            return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
                    .collect(Collectors.joining(", "));
        }

        public String columnNamesAsString(Function<SQLite3Column, String> function) {
            return getColumns().stream().map(function).collect(Collectors.joining(", "));
        }

        public SQLite3RowValue getRandomRowValue(Connection con, SQLite3StateToReproduce state) throws SQLException {
            String randomRow = String.format("SELECT %s, %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." + c.getName() + ")"),
                    tableNamesAsString());
            Map<SQLite3Column, SQLite3Constant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues;
                try {
                    randomRowValues = s.executeQuery(randomRow);
                } catch (SQLException e) {
                    throw new IgnoreMeException();
                }
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n" + state);
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    SQLite3Column column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    String typeString = randomRowValues.getString(columnIndex + getColumns().size());
                    SQLite3DataType valueType = getColumnType(typeString);
                    SQLite3Constant constant = getConstant(randomRowValues, columnIndex, valueType);
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                state.randomRowValues = values;
                return new SQLite3RowValue(this, values);
            }

        }

    }

    public static class SQLite3Table extends AbstractTable<SQLite3Column, TableIndex> {
        // TODO: why does the SQLite implementation have no table indexes?

        public enum TableKind {
            MAIN, TEMP;
        }

        private final TableKind tableType;
        private SQLite3Column rowid;
        private final boolean withoutRowid;
        private final int nrRows;
        private final boolean isVirtual;
        private final boolean isReadOnly;

        public SQLite3Table(String tableName, List<SQLite3Column> columns, TableKind tableType, boolean withoutRowid,
                int nrRows, boolean isView, boolean isVirtual, boolean isReadOnly) {
            super(tableName, columns, Collections.emptyList(), isView);
            this.tableType = tableType;
            this.withoutRowid = withoutRowid;
            this.isVirtual = isVirtual;
            this.isReadOnly = isReadOnly;
            this.nrRows = nrRows;
        }

        public boolean hasWithoutRowid() {
            return withoutRowid;
        }

        public void addRowid(SQLite3Column rowid) {
            this.rowid = rowid;
        }

        public SQLite3Column getRowid() {
            return rowid;
        }

        public TableKind getTableType() {
            return tableType;
        }

        public boolean isVirtual() {
            return isVirtual;
        }

        public boolean isSystemTable() {
            return getName().startsWith("sqlit");
        }

        public int getNrRows() {
            return nrRows;
        }

        public boolean isTemp() {
            return tableType == TableKind.TEMP;
        }

        public boolean isReadOnly() {
            return isReadOnly;
        }

    }

    public static class SQLite3RowValue {
        private final SQLite3Tables tables;
        private final Map<SQLite3Column, SQLite3Constant> values;

        SQLite3RowValue(SQLite3Tables tables, Map<SQLite3Column, SQLite3Constant> values) {
            this.tables = tables;
            this.values = values;
        }

        public SQLite3Tables getTable() {
            return tables;
        }

        public Map<SQLite3Column, SQLite3Constant> getValues() {
            return values;
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            int i = 0;
            for (SQLite3Column c : tables.getColumns()) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                sb.append(values.get(c));
            }
            return sb.toString();
        }

        public String getRowValuesAsString() {
            List<SQLite3Column> columnsToCheck = tables.getColumns();
            return getRowValuesAsString(columnsToCheck);
        }

        public String getRowValuesAsString(List<SQLite3Column> columnsToCheck) {
            StringBuilder sb = new StringBuilder();
            Map<SQLite3Column, SQLite3Constant> expectedValues = getValues();
            for (int i = 0; i < columnsToCheck.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                SQLite3Constant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
                SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
                visitor.visit(expectedColumnValue);
                sb.append(visitor.get());
            }
            return sb.toString();
        }

    }

    public SQLite3Schema(List<SQLite3Table> databaseTables, List<String> indexNames) {
        this.indexNames = indexNames;
        this.databaseTables = Collections.unmodifiableList(databaseTables);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (SQLite3Table t : getDatabaseTables()) {
            sb.append(t);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static int getNrRows(SQLite3GlobalState globalState, String table) throws SQLException {
        String string = "SELECT COUNT(*) FROM " + table;
        List<String> errors = new ArrayList<>();
        errors.add("ORDER BY term out of range");
        errors.addAll(Arrays.asList("second argument to nth_value must be a positive integer",
                "ON clause references tables to its right", "no such table", "no query solution", "no such index",
                "GROUP BY term", "is circularly defined", "misuse of aggregate", "no such column",
                "misuse of window function"));
        SQLite3Errors.addExpectedExpressionErrors(errors);
        QueryAdapter q = new QueryAdapter(string, errors);
        try (ResultSet query = q.executeAndGet(globalState)) {
            if (query == null) {
                throw new IgnoreMeException();
            }
            query.next();
            int int1 = query.getInt(1);
            query.getStatement().close();
            return int1;
        }
    }

    public static SQLite3Schema fromConnection(SQLite3GlobalState globalState) throws SQLException {
        List<SQLite3Table> databaseTables = new ArrayList<>();
        List<String> indexNames = new ArrayList<>();
        Connection con = globalState.getConnection();

        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT name, type as category, sql FROM sqlite_master UNION "
                    + "SELECT name, 'temp_table' as category, sql FROM sqlite_temp_master WHERE type='table' UNION SELECT name, 'view' as category, sql FROM sqlite_temp_master WHERE type='view' GROUP BY name;")) {
                while (rs.next()) {
                    String tableName = rs.getString("name");
                    String tableType = rs.getString("category");
                    boolean isReadOnly;
                    if (databaseTables.stream().anyMatch(t -> t.getName().contentEquals(tableName))) {
                        continue;
                    }
                    String sqlString = rs.getString("sql") == null ? "" : rs.getString("sql").toLowerCase();
                    if (tableName.startsWith("sqlite_") || tableType.equals("index") || tableType.equals("trigger")
                            || tableName.endsWith("_idx") || tableName.endsWith("_docsize")
                            || tableName.endsWith("_content") || tableName.endsWith("_data")
                            || tableName.endsWith("_config") || tableName.endsWith("_segdir")
                            || tableName.endsWith("_stat") || tableName.endsWith("_segments")
                            || tableName.contains("_")) {
                        isReadOnly = true;
                        continue; // TODO
                    } else if (sqlString.contains("using dbstat")) {
                        isReadOnly = true;
                    } else if (sqlString.contains("content=''")) {
                        isReadOnly = true;
                    } else {
                        isReadOnly = false;
                    }
                    boolean withoutRowid = sqlString.contains("without rowid");
                    boolean isView = tableType.contentEquals("view");
                    boolean isVirtual = sqlString.contains("virtual");
                    boolean isDbStatsTable = sqlString.contains("using dbstat");
                    List<SQLite3Column> databaseColumns = getTableColumns(con, tableName, sqlString, isView,
                            isDbStatsTable);
                    int nrRows;
                    try {
                        // FIXME
                        nrRows = getNrRows(globalState, tableName);
                    } catch (IgnoreMeException e) {
                        nrRows = 0;
                    }
                    SQLite3Table t = new SQLite3Table(tableName, databaseColumns,
                            tableType.contentEquals("temp_table") ? TableKind.TEMP : TableKind.MAIN, withoutRowid,
                            nrRows, isView, isVirtual, isReadOnly);
                    if (isRowIdTable(withoutRowid, isView, isVirtual)) {
                        String rowId = Randomly.fromOptions("rowid", "_rowid_", "oid");
                        SQLite3Column rowid = new SQLite3Column(rowId, SQLite3DataType.INT, true, null, true);
                        t.addRowid(rowid);
                        rowid.setTable(t);
                    }
                    for (SQLite3Column c : databaseColumns) {
                        c.setTable(t);
                    }
                    databaseTables.add(t);
                }
            } catch (SQLException e) {
                // ignore
            }
            try (ResultSet rs = s.executeQuery(
                    "SELECT name FROM SQLite_master WHERE type = 'index' UNION SELECT name FROM sqlite_temp_master WHERE type='index'")) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (name.contains("_autoindex")) {
                        continue;
                    }
                    indexNames.add(name);
                }
            }
        }

        return new SQLite3Schema(databaseTables, indexNames);
    }

    // https://www.sqlite.org/rowidtable.html
    private static boolean isRowIdTable(boolean withoutRowid, boolean isView, boolean isVirtual) {
        return !isView && !isVirtual && !withoutRowid;
    }

    private static List<SQLite3Column> getTableColumns(Connection con, String tableName, String sql, boolean isView,
            boolean isDbStatsTable) throws SQLException {
        List<SQLite3Column> databaseColumns = new ArrayList<>();
        try (Statement s2 = con.createStatement()) {
            String tableInfoStr = String.format("PRAGMA table_xinfo(%s)", tableName);
            try (ResultSet columnRs = s2.executeQuery(tableInfoStr)) {
                String[] columnCreates = sql.split(",");
                int columnCreateIndex = 0;
                while (columnRs.next()) {
                    String columnName = columnRs.getString("name");
                    if (columnName.contentEquals("docid") || columnName.contentEquals("rank")
                            || columnName.contentEquals(tableName) || columnName.contentEquals("__langid")) {
                        continue; // internal column names of FTS tables
                    }
                    if (isDbStatsTable && columnName.contentEquals("aggregate")) {
                        // see https://www.sqlite.org/src/tktview?name=a3713a5fca
                        continue;
                    }
                    String columnTypeString = columnRs.getString("type");
                    boolean isPrimaryKey = columnRs.getBoolean("pk");
                    SQLite3DataType columnType = getColumnType(columnTypeString);
                    SQLite3CollateSequence collate;
                    if (!isDbStatsTable) {
                        String columnSql = columnCreates[columnCreateIndex++];
                        collate = getCollate(columnSql, isView);
                    } else {
                        collate = SQLite3CollateSequence.BINARY;
                    }
                    databaseColumns.add(new SQLite3Column(columnName, columnType,
                            columnTypeString.contentEquals("INTEGER"), isPrimaryKey, collate));
                }
            }
        } catch (Exception e) {
        }
        if (databaseColumns.isEmpty()) {
            // only generated columns
            throw new IgnoreMeException();
        }
        assert !databaseColumns.isEmpty() : tableName;
        return databaseColumns;
    }

    private static SQLite3CollateSequence getCollate(String sql, boolean isView) {
        SQLite3CollateSequence collate;
        if (isView) {
            collate = SQLite3CollateSequence.BINARY;
        } else {
            if (sql.contains("collate binary")) {
                collate = SQLite3CollateSequence.BINARY;
            } else if (sql.contains("collate rtrim")) {
                collate = SQLite3CollateSequence.RTRIM;
            } else if (sql.contains("collate nocase")) {
                collate = SQLite3CollateSequence.NOCASE;
            } else {
                collate = SQLite3CollateSequence.BINARY;
            }
        }
        return collate;
    }

    public static SQLite3DataType getColumnType(String columnTypeString) {
        String trimmedTypeString = columnTypeString.toUpperCase().replace(" GENERATED ALWAYS", "");
        SQLite3DataType columnType;
        switch (trimmedTypeString) {
        case "TEXT":
            columnType = SQLite3DataType.TEXT;
            break;
        case "INTEGER":
            columnType = SQLite3DataType.INT;
            break;
        case "INT":
        case "BOOLEAN":
            columnType = SQLite3DataType.INT;
            break;
        case "":
            columnType = SQLite3DataType.NONE;
            break;
        case "BLOB":
            columnType = SQLite3DataType.BINARY;
            break;
        case "REAL":
        case "NUM":
            columnType = SQLite3DataType.REAL;
            break;
        case "NULL":
            columnType = SQLite3DataType.NULL;
            break;
        default:
            throw new AssertionError(trimmedTypeString);
        }
        return columnType;
    }

    public SQLite3Table getRandomTable() {
        return Randomly.fromList(getDatabaseTables());
    }

    public SQLite3Table getRandomTable(Predicate<SQLite3Table> predicate) {
        List<SQLite3Table> collect = databaseTables.stream().filter(predicate).collect(Collectors.toList());
        if (collect.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(collect);
    }

    public List<SQLite3Table> getTables(Predicate<SQLite3Table> predicate) {
        return databaseTables.stream().filter(predicate).collect(Collectors.toList());
    }

    public SQLite3Table getRandomTableOrBailout(Predicate<SQLite3Table> predicate) {
        List<SQLite3Table> tables = databaseTables.stream().filter(predicate).collect(Collectors.toList());
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(tables);
        }
    }

    public SQLite3Table getRandomVirtualTable() {
        return getRandomTable(p -> p.isVirtual);
    }

    public List<SQLite3Table> getDatabaseTables() {
        return databaseTables;
    }

    public SQLite3Tables getTables() {
        return new SQLite3Tables(databaseTables);
    }

    public SQLite3Tables getRandomTableNonEmptyTables() {
        if (databaseTables.isEmpty()) {
            throw new IgnoreMeException();
        }
        return new SQLite3Tables(Randomly.nonEmptySubset(databaseTables));
    }

    public SQLite3Table getRandomTableNoViewOrBailout() {
        List<SQLite3Table> databaseTablesWithoutViews = getDatabaseTablesWithoutViews();
        if (databaseTablesWithoutViews.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(databaseTablesWithoutViews);
    }

    public SQLite3Table getRandomTableNoViewNoVirtualTable() {
        return Randomly.fromList(getDatabaseTablesWithoutViewsWithoutVirtualTables());
    }

    public List<SQLite3Table> getDatabaseTablesWithoutViews() {
        return databaseTables.stream().filter(t -> !t.isView()).collect(Collectors.toList());
    }

    public List<SQLite3Table> getViews() {
        return databaseTables.stream().filter(t -> t.isView()).collect(Collectors.toList());
    }

    public SQLite3Table getRandomViewOrBailout() {
        if (getViews().isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(getViews());
        }
    }

    public List<SQLite3Table> getDatabaseTablesWithoutViewsWithoutVirtualTables() {
        return databaseTables.stream().filter(t -> !t.isView() && !t.isVirtual).collect(Collectors.toList());
    }

}
