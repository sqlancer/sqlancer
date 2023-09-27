package sqlancer.materialize;

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
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeSchema.MaterializeTable.TableType;
import sqlancer.materialize.ast.MaterializeConstant;

public class MaterializeSchema extends AbstractSchema<MaterializeGlobalState, MaterializeTable> {

    private final String databaseName;
    private final List<String> indexNames;

    public List<String> getIndexNames() {
        return indexNames;
    }

    public enum MaterializeDataType {
        INT, BOOLEAN, TEXT, DECIMAL, FLOAT, REAL, BIT;

        public static MaterializeDataType getRandomType() {
            List<MaterializeDataType> dataTypes = new ArrayList<>(Arrays.asList(values()));
            if (MaterializeProvider.generateOnlyKnown) {
                dataTypes.remove(MaterializeDataType.DECIMAL);
                dataTypes.remove(MaterializeDataType.FLOAT);
                dataTypes.remove(MaterializeDataType.REAL);
                dataTypes.remove(MaterializeDataType.BIT);
            }
            return Randomly.fromList(dataTypes);
        }
    }

    public static class MaterializeColumn extends AbstractTableColumn<MaterializeTable, MaterializeDataType> {

        public MaterializeColumn(String name, MaterializeDataType columnType) {
            super(name, null, columnType);
        }

        public static MaterializeColumn createDummy(String name) {
            return new MaterializeColumn(name, MaterializeDataType.INT);
        }

    }

    public static class MaterializeTables extends AbstractTables<MaterializeTable, MaterializeColumn> {

        public MaterializeTables(List<MaterializeTable> tables) {
            super(tables);
        }

        public MaterializeRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    tableNamesAsString());
            Map<MaterializeColumn, MaterializeConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    MaterializeColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    MaterializeConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = MaterializeConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                        case INT:
                            constant = MaterializeConstant.createIntConstant(randomRowValues.getLong(columnIndex));
                            break;
                        case BOOLEAN:
                            constant = MaterializeConstant
                                    .createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                            break;
                        case TEXT:
                            constant = MaterializeConstant.createTextConstant(randomRowValues.getString(columnIndex));
                            break;
                        default:
                            throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new MaterializeRowValue(this, values);
            } catch (PSQLException e) {
                throw new IgnoreMeException();
            }

        }

    }

    public static MaterializeDataType getColumnType(String typeString) {
        switch (typeString) {
        case "smallint":
        case "integer":
        case "bigint":
            return MaterializeDataType.INT;
        case "boolean":
            return MaterializeDataType.BOOLEAN;
        case "text":
        case "character":
        case "character varying":
        case "name":
        case "regclass":
            return MaterializeDataType.TEXT;
        case "numeric":
            return MaterializeDataType.DECIMAL;
        case "double precision":
            return MaterializeDataType.FLOAT;
        case "real":
            return MaterializeDataType.REAL;
        case "bit":
            return MaterializeDataType.BIT;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class MaterializeRowValue
            extends AbstractRowValue<MaterializeTables, MaterializeColumn, MaterializeConstant> {

        protected MaterializeRowValue(MaterializeTables tables, Map<MaterializeColumn, MaterializeConstant> values) {
            super(tables, values);
        }

    }

    public static class MaterializeTable
            extends AbstractRelationalTable<MaterializeColumn, MaterializeIndex, MaterializeGlobalState> {

        public enum TableType {
            STANDARD, TEMPORARY
        }

        private final TableType tableType;
        private final List<MaterializeStatisticsObject> statistics;
        private final boolean isInsertable;

        public MaterializeTable(String tableName, List<MaterializeColumn> columns, List<MaterializeIndex> indexes,
                TableType tableType, List<MaterializeStatisticsObject> statistics, boolean isView,
                boolean isInsertable) {
            super(tableName, columns, indexes, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
        }

        public List<MaterializeStatisticsObject> getStatistics() {
            return statistics;
        }

        public TableType getTableType() {
            return tableType;
        }

        public boolean isInsertable() {
            return isInsertable;
        }

    }

    public static final class MaterializeStatisticsObject {
        private final String name;

        public MaterializeStatisticsObject(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static final class MaterializeIndex extends TableIndex {

        private MaterializeIndex(String indexName) {
            super(indexName);
        }

        public static MaterializeIndex create(String indexName) {
            return new MaterializeIndex(indexName);
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

    public static MaterializeSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        try {
            List<MaterializeTable> databaseTables = new ArrayList<>();
            List<String> indexNames = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                // ERROR: column "is_insertable_into" does not exist
                try (ResultSet rs = s.executeQuery(
                        "SELECT table_name, table_schema, table_type FROM information_schema.tables WHERE table_schema='public' OR table_schema LIKE 'pg_temp_%' ORDER BY table_name;")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String tableTypeSchema = rs.getString("table_schema");
                        boolean isInsertable = true;
                        String type = rs.getString("table_type");
                        boolean isView = type.equals("VIEW") || type.equals("MATERIALIZED VIEW");
                        if (isView) {
                            isInsertable = false;
                        }
                        MaterializeTable.TableType tableType = getTableType(tableTypeSchema);
                        List<MaterializeColumn> databaseColumns = getTableColumns(con, tableName);
                        List<MaterializeIndex> indexes = getIndexes(con, tableName);
                        List<MaterializeStatisticsObject> statistics = getStatistics(con);
                        MaterializeTable t = new MaterializeTable(tableName, databaseColumns, indexes, tableType,
                                statistics, isView, isInsertable);
                        for (MaterializeColumn c : databaseColumns) {
                            c.setTable(t);
                        }
                        databaseTables.add(t);
                    }
                }
            }
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(String.format(
                        "SELECT mz_indexes.name, mz_databases.name FROM mz_indexes JOIN mz_relations ON mz_indexes.on_id = mz_relations.id JOIN mz_schemas ON mz_relations.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id WHERE mz_databases.name = '%s';",
                        databaseName))) {
                    while (rs.next()) {
                        String name = rs.getString(1);
                        indexNames.add(name);
                    }
                }
            }
            return new MaterializeSchema(databaseTables, databaseName, indexNames);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
    }

    protected static List<MaterializeStatisticsObject> getStatistics(SQLConnection con) throws SQLException {
        return new ArrayList<>();
    }

    protected static MaterializeTable.TableType getTableType(String tableTypeStr) throws AssertionError {
        MaterializeTable.TableType tableType;
        if (tableTypeStr.contentEquals("public")) {
            tableType = TableType.STANDARD;
        } else if (tableTypeStr.startsWith("pg_temp")) {
            tableType = TableType.TEMPORARY;
        } else {
            throw new AssertionError(tableTypeStr);
        }
        return tableType;
    }

    protected static List<MaterializeIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<MaterializeIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String
                    // org.postgresql.util.PSQLException: ERROR: unknown catalog item 'pg_indexes'
                    .format("SELECT c.relname as indexname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid WHERE c.relkind IN ('i','I','') AND n.nspname <> 'pg_catalog' AND n.nspname !~ '^pg_toast' AND n.nspname <> 'information_schema' AND c2.relname = '%s' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY indexname;",
                            tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("indexname");
                    if (DBMSCommon.matchesIndexName(indexName)) {
                        indexes.add(MaterializeIndex.create(indexName));
                    }
                }
            }
        }
        return indexes;
    }

    protected static List<MaterializeColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<MaterializeColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '"
                            + tableName + "' ORDER BY column_name")) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    MaterializeColumn c = new MaterializeColumn(columnName, getColumnType(dataType));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public MaterializeSchema(List<MaterializeTable> databaseTables, String databaseName, List<String> indexNames) {
        super(databaseTables);
        this.databaseName = databaseName;
        this.indexNames = indexNames;
    }

    public MaterializeTables getRandomTableNonEmptyTables() {
        return new MaterializeTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public String getDatabaseName() {
        return databaseName;
    }

}
