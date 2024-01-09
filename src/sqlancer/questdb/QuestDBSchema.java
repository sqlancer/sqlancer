package sqlancer.questdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;

public class QuestDBSchema extends AbstractSchema<QuestDBGlobalState, QuestDBTable> {

    public QuestDBSchema(List<QuestDBTable> databaseTables) {
        super(databaseTables);
    }

    public static QuestDBSchema fromConnection(SQLConnection con) throws SQLException {
        List<QuestDBTable> databaseTables = new ArrayList<>();
        for (String tableName : getTableNames(con)) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<QuestDBColumn> databaseColumns = getTableColumns(con, tableName);
            QuestDBTable t = new QuestDBTable(tableName, databaseColumns);
            for (QuestDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);
        }
        return new QuestDBSchema(databaseTables);
    }

    protected static List<String> getTableNames(SQLConnection con) throws SQLException {
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT name FROM tables()")) {
                List<String> tableNames = new ArrayList<>();
                while (rs.next()) {
                    String tName = rs.getString("name");
                    // exclude system tables for testing
                    if (!QuestDBTables.isSystemTable(tName)) {
                        tableNames.add(tName);
                    }
                }
                return tableNames;
            }
        }
    }

    private static List<QuestDBColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        try (Statement s = con.createStatement()) {
            String columnsStmt = "SELECT column, type, indexed, designated FROM table_columns('" + tableName + "')";
            try (ResultSet rs = s.executeQuery(columnsStmt)) {
                List<QuestDBColumn> columns = new ArrayList<>();
                while (rs.next()) {
                    String columnName = rs.getString("column");
                    QuestDBDataType dataType = QuestDBDataType.valueOf(rs.getString("type"));
                    boolean isIndexed = rs.getBoolean("indexed");
                    boolean isDesignated = rs.getBoolean("designated");
                    columns.add(new QuestDBColumn(columnName, dataType, isIndexed, isDesignated));
                }
                return columns;
            }
        }
    }

    public QuestDBTables getRandomTableNonEmptyTables() {
        return new QuestDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public static class QuestDBColumn extends AbstractTableColumn<QuestDBTable, QuestDBDataType> {
        private final boolean isIndexed;
        private final boolean isDesignated;

        public QuestDBColumn(String name, QuestDBDataType columnType, boolean isIndexed, boolean isDesignated) {
            super(name, null, columnType);
            this.isIndexed = isIndexed;
            this.isDesignated = isDesignated;
        }

        public boolean isIndexed() {
            return isIndexed;
        }

        public boolean isDesignated() {
            return isDesignated;
        }

        public boolean isNullable() {
            return getType().isNullable;
        }
    }

    public static class QuestDBTables extends AbstractTables<QuestDBTable, QuestDBColumn> {
        public QuestDBTables(List<QuestDBTable> tables) {
            super(tables);
        }

        public static boolean isSystemTable(String tName) {
            return tName != null && (tName.startsWith("sys.") || tName.startsWith("telemetry"));
        }
    }

    public static class QuestDBTable extends AbstractRelationalTable<QuestDBColumn, TableIndex, QuestDBGlobalState> {
        private final SQLQueryAdapter numRowsStmt;

        public QuestDBTable(String tableName, List<QuestDBColumn> columns) {
            super(tableName, columns, Collections.emptyList(), false);
            numRowsStmt = new SQLQueryAdapter("SELECT COUNT(*) FROM '" + name + '\'');
        }

        @Override
        public long getNrRows(QuestDBGlobalState globalState) {
            if (rowCount != NO_ROW_COUNT_AVAILABLE) {
                return rowCount;
            }
            try (SQLancerResultSet query = numRowsStmt.executeAndGet(globalState)) {
                if (query != null) {
                    query.next();
                    return (rowCount = query.getLong(1));
                }
            } catch (SQLException ignore) {
                // fall through
            }
            throw new IgnoreMeException();
        }
    }
}
