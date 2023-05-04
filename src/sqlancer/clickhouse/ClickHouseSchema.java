package sqlancer.clickhouse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.constant.ClickHouseCreateConstant;
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
            this.clickHouseType = ClickHouseDataType.of(textRepr);
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
                boolean isMaterialized, ClickHouseTable table) {
            super(name, table, columnType);
            this.isAlias = isAlias;
            this.isMaterialized = isMaterialized;
        }

        public static ClickHouseSchema.ClickHouseColumn createDummy(String name, ClickHouseTable table) {
            return new ClickHouseSchema.ClickHouseColumn(name, ClickHouseLancerDataType.getRandom(), false, false,
                    table);
        }

        public boolean isAlias() {
            return isAlias;
        }

        public boolean isMaterialized() {
            return isMaterialized;
        }

        public ClickHouseColumnReference asColumnReference(String tableAlias) {
            return new ClickHouseColumnReference(this, null, tableAlias);
        }

    }

    public static ClickHouseConstant getConstant(ResultSet randomRowValues, int columnIndex,
            ClickHouseDataType valueType) throws SQLException, AssertionError {
        Object value;
        ClickHouseConstant constant;
        if (randomRowValues.getString(columnIndex) == null) {
            constant = ClickHouseCreateConstant.createNullConstant();
        } else {
            switch (valueType) {
            case Int32:
                value = randomRowValues.getLong(columnIndex);
                constant = ClickHouseCreateConstant.createInt32Constant((long) value);
                break;
            case Float64:
                value = randomRowValues.getDouble(columnIndex);
                constant = ClickHouseCreateConstant.createFloat64Constant((double) value);
                break;
            case String:
                value = randomRowValues.getString(columnIndex);
                constant = ClickHouseCreateConstant.createStringConstant((String) value);
                break;
            case AggregateFunction:
            case Array:
                // case Bool:
            case Date:
                // case Date32:
            case DateTime:
            case DateTime32:
            case DateTime64:
            case Decimal:
            case Decimal128:
            case Decimal256:
            case Decimal32:
            case Decimal64:
                // case Enum:
            case Enum16:
            case Enum8:
            case FixedString:
            case Float32:
            case IPv4:
            case IPv6:
            case Int128:
            case Int16:
            case Int256:
            case Int64:
            case Int8:
            case IntervalDay:
            case IntervalHour:
                // case IntervalMicrosecond:
                // case IntervalMillisecond:
            case IntervalMinute:
            case IntervalMonth:
                // case IntervalNanosecond:
            case IntervalQuarter:
            case IntervalSecond:
            case IntervalWeek:
            case IntervalYear:
                // case JSON:
                // case LowCardinality:
            case Map:
                // case MultiPolygon:
            case Nested:
            case Nothing:
                // case Nullable:
                // case Object:
                // case Point:
                // case Polygon:
                // case Ring:
                // case SimpleAggregateFunction:
            case Tuple:
            case UInt128:
            case UInt16:
            case UInt256:
            case UInt32:
            case UInt64:
            case UInt8:
            case UUID:
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
                            isMaterialized, null);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
