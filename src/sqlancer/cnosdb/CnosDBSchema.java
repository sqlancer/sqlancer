package sqlancer.cnosdb;

import sqlancer.Randomly;
import sqlancer.cnosdb.ast.CnosDBConstant;
import sqlancer.cnosdb.client.CnosDBConnection;
import sqlancer.cnosdb.client.CnosDBResultSet;
import sqlancer.common.schema.*;

import java.util.*;

public class CnosDBSchema extends AbstractSchema<CnosDBGlobalState, CnosDBSchema.CnosDBTable> {

    private final String databaseName;

    public enum CnosDBDataType {
        INT, BOOLEAN, STRING, DOUBLE, UINT, TIMESTAMP;

        public static CnosDBDataType getRandomType() {
            return Randomly.fromOptions(values());
        }

        public static CnosDBDataType getRandomTypeWithoutTimeStamp() {
            List<CnosDBDataType> dataTypes = new ArrayList<>(Arrays.asList(values()));
            dataTypes.remove(TIMESTAMP);
            return Randomly.fromList(dataTypes);
        }
    }

    public static class CnosDBColumn extends AbstractTableColumn<CnosDBTable, CnosDBDataType> {

        public CnosDBColumn(String name, CnosDBDataType columnType) {
            super(name, null, columnType);
        }

        public static CnosDBColumn createDummy(String name) {
            return new CnosDBColumn(name, CnosDBDataType.INT);
        }

    }

    public static class CnosDBTagColumn extends CnosDBColumn {
        public CnosDBTagColumn(String name) {
            super(name, CnosDBDataType.STRING);
        }
    }

    public static class CnosDBTimeColumn extends CnosDBColumn {
        public CnosDBTimeColumn() {
            super("TIME", CnosDBDataType.TIMESTAMP);
        }
    }

    public static class CnosDBFieldColumn extends CnosDBColumn {
        public CnosDBFieldColumn(String name, CnosDBDataType columnType) {
            super(name, columnType);
            assert columnType != CnosDBDataType.TIMESTAMP;
        }
    }

    public static class CnosDBTables extends AbstractTables<CnosDBTable, CnosDBColumn> {

        public CnosDBTables(List<CnosDBTable> tables) {
            super(tables);
        }

        public CnosDBRowValue getRandomRowValue(CnosDBConnection con) {
            return null;
        }

        public List<CnosDBColumn> getRandomColumnsWithOnlyOneField() {
            ArrayList<CnosDBColumn> res = new ArrayList<>();
            this.getTables().forEach(table -> res.addAll(table.getRandomColumnsWithOnlyOneField()));
            return res;
        }

    }

    public static CnosDBDataType getColumnType(String typeString) {
        switch (typeString.toLowerCase()) {
        case "bigint":
            return CnosDBDataType.INT;
        case "boolean":
            return CnosDBDataType.BOOLEAN;
        case "string":
            return CnosDBDataType.STRING;
        case "double":
            return CnosDBDataType.DOUBLE;
        case "bigint unsigned":
        case "unsigned":
            return CnosDBDataType.UINT;
        case "timestamp":
            return CnosDBDataType.TIMESTAMP;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class CnosDBRowValue extends AbstractRowValue<CnosDBTables, CnosDBColumn, CnosDBConstant> {

        protected CnosDBRowValue(CnosDBTables tables, Map<CnosDBColumn, CnosDBConstant> values) {
            super(tables, values);
        }

    }

    public static class CnosDBTable extends AbstractTable<CnosDBColumn, TableIndex, CnosDBGlobalState> {

        public CnosDBTable(String tableName, List<CnosDBColumn> columns) {
            super(tableName, columns, null, false);
        }

        @Override
        public List<CnosDBColumn> getColumns() {
            List<CnosDBColumn> res = super.getColumns();
            boolean hasTime = false;
            for (CnosDBColumn column : res) {
                if (column instanceof CnosDBTimeColumn) {
                    hasTime = true;
                    break;
                }
            }
            assert hasTime;

            return res;
        }

        public List<CnosDBColumn> getRandomColumnsWithOnlyOneField() {
            ArrayList<CnosDBColumn> res = new ArrayList<>();
            boolean hasField = false;
            for (CnosDBColumn column : getColumns()) {
                if (column instanceof CnosDBTagColumn && Randomly.getBoolean()) {
                    res.add(column);
                } else if (column instanceof CnosDBFieldColumn && !hasField) {
                    res.add(column);
                    hasField = true;
                }
            }
            return res;
        }

        // SELECT COUNT(*) FROM table;
        @Override
        public long getNrRows(CnosDBGlobalState globalState) {
            long res;
            try {
                CnosDBResultSet tableCountRes = globalState.getConnection().getClient()
                        .executeQuery("SELECT COUNT(time) FROM " + this.name);
                tableCountRes.next();
                res = tableCountRes.getLong(1);
            } catch (Exception e) {
                res = 0;
            }
            return res;
        }

        public List<CnosDBColumn> getRandomNonEmptyColumnSubset() {
            List<CnosDBColumn> selectedColumns = new ArrayList<>();
            ArrayList<CnosDBColumn> remainingColumns = new ArrayList<>(this.getColumns());

            remainingColumns.removeIf(column -> column instanceof CnosDBTimeColumn);
            CnosDBTimeColumn timeColumn = new CnosDBTimeColumn();
            timeColumn.setTable(this);
            selectedColumns.add(timeColumn);

            remainingColumns.stream().filter(column -> column instanceof CnosDBTagColumn).findFirst().ifPresent(tag -> {
                selectedColumns.add(tag);
                remainingColumns.remove(tag);
            });

            remainingColumns.stream().filter(column -> column instanceof CnosDBFieldColumn).findFirst()
                    .ifPresent(field -> {
                        selectedColumns.add(field);
                        remainingColumns.remove(field);
                    });

            int nr = Math.min(Randomly.smallNumber() + 1, remainingColumns.size());
            for (int i = 0; i < nr; i++) {
                selectedColumns
                        .add(remainingColumns.remove((int) Randomly.getNotCachedInteger(0, remainingColumns.size())));
            }
            return selectedColumns;
        }

    }

    public static CnosDBSchema fromConnection(CnosDBConnection con) throws Exception {
        CnosDBResultSet tablesRes = con.getClient().executeQuery("SHOW TABLES");

        List<CnosDBTable> tables = new ArrayList<>();
        while (tablesRes.next()) {
            String tableName = tablesRes.getString(1);
            List<CnosDBColumn> columns = getTableColumns(con, tableName);
            tables.add(new CnosDBTable(tableName, columns));
        }

        return new CnosDBSchema(tables, con.getClient().getDatabase());
    }

    protected static List<CnosDBColumn> getTableColumns(CnosDBConnection con, String tableName) throws Exception {
        CnosDBResultSet columnsRes = con.getClient().executeQuery("DESCRIBE TABLE " + tableName);
        List<CnosDBColumn> columns = new ArrayList<>();
        CnosDBTable table = new CnosDBTable(tableName, columns);
        while (columnsRes.next()) {
            String columnName = columnsRes.getString(1);
            String columnType = columnsRes.getString(3).toLowerCase();
            CnosDBDataType dataType = CnosDBSchema.getColumnType(columnsRes.getString(2));
            CnosDBColumn column;
            if (columnType.contentEquals("time")) {
                column = new CnosDBTimeColumn();
            } else if (columnType.contentEquals("tag")) {
                column = new CnosDBTagColumn(columnName);
            } else {
                column = new CnosDBFieldColumn(columnName, dataType);
            }
            column.setTable(table);
            columns.add(column);
        }

        return columns;
    }

    public CnosDBSchema(List<CnosDBTable> databaseTables, String databaseName) {
        super(databaseTables);
        this.databaseName = databaseName;
    }

    public CnosDBTables getRandomTableNonEmptyTables() {
        return new CnosDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public String getDatabaseName() {
        return databaseName;
    }

}
