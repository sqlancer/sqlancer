package sqlancer.clickhouse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;

public class ClickHouseSchema extends AbstractSchema<ClickHouseGlobalState, ClickHouseTable> {

    public static class ClickHouseLancerDataType {

        private final ClickHouseDataType clickHouseType;
        private final String textRepr;

        public ClickHouseLancerDataType(ClickHouseDataType type) {
            this.clickHouseType = type;
            this.textRepr = type.toString();
        }

        public ClickHouseLancerDataType(String textRepr) {
            this.clickHouseType = ClickHouseDataType.fromTypeString(textRepr);
            this.textRepr = textRepr;
        }

        public static ClickHouseLancerDataType getRandom() {
            return new ClickHouseLancerDataType(
                    Randomly.fromOptions(ClickHouseDataType.Int32, ClickHouseDataType.String));
        }

        public ClickHouseDataType getType() {
            return clickHouseType;
        }

        @Override
        public String toString() {
            return textRepr;
        }

    }

    public static class ClickHouseColumn extends AbstractTableColumn<ClickHouseTable, ClickHouseLancerDataType> {

        private final boolean isAlias;
        private final boolean isMaterialized;

        public ClickHouseColumn(String name, ClickHouseLancerDataType columnType, boolean isAlias,
                boolean isMaterialized) {
            super(name, null, columnType);
            this.isAlias = isAlias;
            this.isMaterialized = isMaterialized;
        }

        public static ClickHouseSchema.ClickHouseColumn createDummy(String name) {
            return new ClickHouseSchema.ClickHouseColumn(name, ClickHouseLancerDataType.getRandom(), false, false);
        }

        public boolean isAlias() {
            return isAlias;
        }

        public boolean isMaterialized() {
            return isMaterialized;
        }
    }

    public static ClickHouseConstant getConstant(ResultSet randomRowValues, int columnIndex,
            ClickHouseDataType valueType) throws SQLException, AssertionError {
        Object value;
        ClickHouseConstant constant;
        if (randomRowValues.getString(columnIndex) == null) {
            constant = ClickHouseConstant.createNullConstant();
        } else {
            switch (valueType) {
            case Int32:
                value = randomRowValues.getLong(columnIndex);
                constant = ClickHouseConstant.createInt32Constant((long) value);
                break;
            case Float64:
                value = randomRowValues.getDouble(columnIndex);
                constant = ClickHouseConstant.createFloat64Constant((double) value);
                break;
            case String:
                value = randomRowValues.getString(columnIndex);
                constant = ClickHouseConstant.createStringConstant((String) value);
                break;
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal:
            case UUID:
            case FixedString:
            case Nothing:
            case Nested:
            case Tuple:
            case Int16:
            case Int8:
            case Date:
            case DateTime:
            case Enum8:
            case Enum16:
            case Float32:
            case Array:
            case AggregateFunction:
            case Unknown:
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case UInt64:
            case UInt32:
            case UInt16:
            case UInt8:
            case Int64:
            default:
                throw new AssertionError(valueType);
            }
        }
        return constant;
    }

    public static class ClickHouseRowValue
            extends AbstractRowValue<ClickHouseTables, ClickHouseColumn, ClickHouseConstant> {

        ClickHouseRowValue(ClickHouseSchema.ClickHouseTables tables,
                Map<ClickHouseSchema.ClickHouseColumn, ClickHouseConstant> values) {
            super(tables, values);
        }

    }

    public static class ClickHouseTables extends AbstractTables<ClickHouseTable, ClickHouseColumn> {

        public ClickHouseTables(List<ClickHouseSchema.ClickHouseTable> tables) {
            super(tables);
        }

    }

    public ClickHouseSchema(List<ClickHouseTable> databaseTables) {
        super(databaseTables);
    }

    public ClickHouseTables getRandomTableNonEmptyTables() {
        return new ClickHouseTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static ClickHouseLancerDataType getColumnType(String typeString) {
        return new ClickHouseLancerDataType(typeString);
    }

    public static class ClickHouseTable
            extends AbstractRelationalTable<ClickHouseColumn, TableIndex, ClickHouseGlobalState> {

        public ClickHouseTable(String tableName, List<ClickHouseColumn> columns, List<TableIndex> indexes,
                boolean isView) {
            super(tableName, columns, indexes, isView);
        }
    }

    public static ClickHouseSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<ClickHouseTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            List<ClickHouseColumn> databaseColumns = getTableColumns(con, tableName);
            List<TableIndex> indexes = Collections.emptyList();
            boolean isView = tableName.startsWith("v");
            ClickHouseTable t = new ClickHouseTable(tableName, databaseColumns, indexes, isView);
            for (ClickHouseColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new ClickHouseSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            ResultSet tableRs = s.executeQuery("SHOW TABLES");
            while (tableRs.next()) {
                String tableName = tableRs.getString(1);
                tableNames.add(tableName);
            }
        }
        return tableNames;
    }

    private static List<ClickHouseColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<ClickHouseColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("DESCRIBE " + tableName)) {
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String dataType = rs.getString("type");
                    String defaultType = rs.getString("default_type");
                    boolean isAlias = "ALIAS".compareTo(defaultType) == 0;
                    boolean isMaterialized = "MATERIALIZED".compareTo(defaultType) == 0;
                    ClickHouseColumn c = new ClickHouseColumn(columnName, getColumnType(dataType), isAlias,
                            isMaterialized);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
