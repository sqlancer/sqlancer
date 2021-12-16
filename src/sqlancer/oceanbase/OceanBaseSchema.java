package sqlancer.oceanbase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.ast.OceanBaseConstant;

public class OceanBaseSchema extends AbstractSchema<OceanBaseGlobalState, OceanBaseTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum OceanBaseDataType {
        INT, VARCHAR, FLOAT, DOUBLE, DECIMAL;

        public static OceanBaseDataType getRandom(OceanBaseGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(OceanBaseDataType.INT, OceanBaseDataType.VARCHAR);
            } else {
                return Randomly.fromOptions(values());
            }
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

    public static class OceanBaseColumn extends AbstractTableColumn<OceanBaseTable, OceanBaseDataType> {

        private final boolean isPrimaryKey;
        private final boolean isZeroFill;
        private final int precision;
        public boolean isPartioned;

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());

            }
        }

        public OceanBaseColumn(String name, OceanBaseDataType columnType, boolean isPrimaryKey, int precision,
                boolean isZeroFill) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
            this.isPartioned = true;
            this.isZeroFill = isZeroFill;
        }

        public int getPrecision() {
            return precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isZeroFill() {
            return isZeroFill;
        }

    }

    public static class OceanBaseTables extends AbstractTables<OceanBaseTable, OceanBaseColumn> {

        public OceanBaseTables(List<OceanBaseTable> tables) {
            super(tables);
        }

        public OceanBaseRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1",
                    columnNamesAsString(c -> c.getType() == OceanBaseDataType.FLOAT || c.isZeroFill()
                            ? "concat(" + c.getTable().getName() + "." + c.getName() + ",'')" + " AS "
                                    + c.getTable().getName() + c.getName()
                            : c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName()
                                    + c.getName()),
                    tableNamesAsString());
            // cast float and zerofill as varchar
            Map<OceanBaseColumn, OceanBaseConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new IgnoreMeException();
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    OceanBaseColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    OceanBaseConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        if (column.isZeroFill()) {
                            constant = OceanBaseConstant.createStringConstant("null");
                        } else {
                            constant = OceanBaseConstant.createNullConstant();
                        }
                    } else {
                        switch (column.getType()) {
                        case INT:
                            // cast zerofill as varchar
                            if (column.isZeroFill()) {
                                value = randomRowValues.getString(columnIndex);
                                constant = OceanBaseConstant.createStringConstant((String) value);
                            } else {
                                value = randomRowValues.getLong(columnIndex);
                                constant = OceanBaseConstant.createIntConstant((long) value);
                            }
                            break;
                        case VARCHAR:
                            value = randomRowValues.getString(columnIndex);
                            constant = OceanBaseConstant.createStringConstant((String) value);
                            break;
                        default:
                            throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new OceanBaseRowValue(this, values);
            }

        }

    }

    private static OceanBaseDataType getColumnType(String typeString) {
        switch (typeString) {
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "int":
        case "bigint":
            return OceanBaseDataType.INT;
        case "varchar":
        case "tinytext":
        case "mediumtext":
        case "text":
        case "longtext":
            return OceanBaseDataType.VARCHAR;
        case "double":
            return OceanBaseDataType.DOUBLE;
        case "float":
            return OceanBaseDataType.FLOAT;
        case "decimal":
            return OceanBaseDataType.DECIMAL;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class OceanBaseRowValue
            extends AbstractRowValue<OceanBaseTables, OceanBaseColumn, OceanBaseConstant> {

        OceanBaseRowValue(OceanBaseTables tables, Map<OceanBaseColumn, OceanBaseConstant> values) {
            super(tables, values);
        }

    }

    public static class OceanBaseTable
            extends AbstractRelationalTable<OceanBaseColumn, OceanBaseIndex, OceanBaseGlobalState> {

        public OceanBaseTable(String tableName, List<OceanBaseColumn> columns, List<OceanBaseIndex> indexes) {
            super(tableName, columns, indexes, false);
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static final class OceanBaseIndex extends TableIndex {

        private OceanBaseIndex(String indexName) {
            super(indexName);
        }

        public static OceanBaseIndex create(String indexName) {
            return new OceanBaseIndex(indexName);
        }
    }

    public static OceanBaseSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<OceanBaseTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s
                            .executeQuery("select TABLE_NAME from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            List<OceanBaseColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<OceanBaseIndex> indexes = getIndexes(con, tableName, databaseName);
                            OceanBaseTable t = new OceanBaseTable(tableName, databaseColumns, indexes);
                            for (OceanBaseColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                return new OceanBaseSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<OceanBaseIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<OceanBaseIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    if (!indexName.equals("PRIMARY")) {
                        indexes.add(OceanBaseIndex.create(indexName));
                    }
                }
            }
        }
        return indexes;
    }

    private static List<OceanBaseColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<OceanBaseColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    boolean isZeroFill = rs.getString("COLUMN_TYPE").contains("zerofill");

                    OceanBaseColumn c = new OceanBaseColumn(columnName, getColumnType(dataType), isPrimaryKey,
                            precision, isZeroFill);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public OceanBaseSchema(List<OceanBaseTable> databaseTables) {
        super(databaseTables);
    }

    public OceanBaseTables getRandomTableNonEmptyTables() {
        return new OceanBaseTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
