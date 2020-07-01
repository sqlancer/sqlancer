package sqlancer.clickhouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.schema.AbstractSchema;
import sqlancer.schema.AbstractTable;
import sqlancer.schema.AbstractTableColumn;
import sqlancer.schema.TableIndex;

public class ClickHouseSchema extends AbstractSchema<ClickHouseTable> {

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

        public ClickHouseColumn(String name, ClickHouseLancerDataType columnType) {
            super(name, null, columnType);
        }

    }

    public static ClickHouseConstant getConstant(ResultSet randomRowValues, int columnIndex,
            ClickHouseDataType valueType) throws SQLException, AssertionError {
        Object value;
        ClickHouseConstant constant;
        if (randomRowValues.getString(columnIndex) == null) {
            value = null;
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

    public static class ClickHouseRowValue {
        private final ClickHouseSchema.ClickHouseTables tables;
        private final Map<ClickHouseSchema.ClickHouseColumn, ClickHouseConstant> values;

        ClickHouseRowValue(ClickHouseSchema.ClickHouseTables tables,
                Map<ClickHouseSchema.ClickHouseColumn, ClickHouseConstant> values) {
            this.tables = tables;
            this.values = values;
        }

        public ClickHouseSchema.ClickHouseTables getTable() {
            return tables;
        }

        public Map<ClickHouseSchema.ClickHouseColumn, ClickHouseConstant> getValues() {
            return values;
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            int i = 0;
            for (ClickHouseSchema.ClickHouseColumn c : tables.getColumns()) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                sb.append(values.get(c));
            }
            return sb.toString();
        }

        public String getRowValuesAsString() {
            List<ClickHouseSchema.ClickHouseColumn> columnsToCheck = tables.getColumns();
            return getRowValuesAsString(columnsToCheck);
        }

        public String getRowValuesAsString(List<ClickHouseSchema.ClickHouseColumn> columnsToCheck) {
            StringBuilder sb = new StringBuilder();
            Map<ClickHouseSchema.ClickHouseColumn, ClickHouseConstant> expectedValues = getValues();
            for (int i = 0; i < columnsToCheck.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                ClickHouseConstant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
                ClickHouseToStringVisitor visitor = new ClickHouseToStringVisitor();
                visitor.visit(expectedColumnValue);
                sb.append(visitor.get());
            }
            return sb.toString();
        }

    }

    public static class ClickHouseTables {
        private final List<ClickHouseSchema.ClickHouseTable> tables;
        private final List<ClickHouseSchema.ClickHouseColumn> columns;

        public ClickHouseTables(List<ClickHouseSchema.ClickHouseTable> tables) {
            this.tables = tables;
            columns = new ArrayList<>();
            for (ClickHouseSchema.ClickHouseTable t : tables) {
                columns.addAll(t.getColumns());
            }
        }

        public String tableNamesAsString() {
            return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
        }

        public List<ClickHouseSchema.ClickHouseTable> getTables() {
            return tables;
        }

        public List<ClickHouseSchema.ClickHouseColumn> getColumns() {
            return columns;
        }

        public String columnNamesAsString() {
            return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
                    .collect(Collectors.joining(", "));
        }

        public String columnNamesAsString(Function<ClickHouseSchema.ClickHouseColumn, String> function) {
            return getColumns().stream().map(function).collect(Collectors.joining(", "));
        }

        public ClickHouseRowValue getRandomRowValue(Connection con, StateToReproduce.ClickHouseStateToReproduce state)
                throws SQLException {
            String randomRow = String.format("SELECT %s, %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." + c.getName() + ")"),
                    tableNamesAsString());
            Map<ClickHouseColumn, ClickHouseConstant> values = new HashMap<>();
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
                    ClickHouseSchema.ClickHouseColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    String typeString = randomRowValues.getString(columnIndex + getColumns().size());
                    ClickHouseDataType valueType = getColumnType(typeString).getType();
                    ClickHouseConstant constant = getConstant(randomRowValues, columnIndex, valueType);
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                state.randomRowValues = values;
                return new ClickHouseSchema.ClickHouseRowValue(this, values);
            }

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

    public static class ClickHouseTable extends AbstractTable<ClickHouseColumn, TableIndex> {

        public ClickHouseTable(String tableName, List<ClickHouseColumn> columns, List<TableIndex> indexes,
                boolean isView) {
            super(tableName, columns, indexes, isView);
        }
    }

    public static ClickHouseSchema fromConnection(Connection con, String databaseName) throws SQLException {
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

    private static List<String> getTableNames(Connection con) throws SQLException {
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

    private static List<ClickHouseColumn> getTableColumns(Connection con, String tableName) throws SQLException {
        List<ClickHouseColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("DESCRIBE " + tableName)) {
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String dataType = rs.getString("type");
                    ClickHouseColumn c = new ClickHouseColumn(columnName, getColumnType(dataType));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
