package sqlancer.datafusion;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.ast.DataFusionConstant;
import sqlancer.datafusion.ast.DataFusionExpression;

public class DataFusionSchema extends AbstractSchema<DataFusionGlobalState, DataFusionTable> {

    public DataFusionSchema(List<DataFusionTable> databaseTables) {
        super(databaseTables);
    }

    // update existing tables in DB by query again
    // (like `show tables;`)
    public static DataFusionSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<DataFusionTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);

        for (String tableName : tableNames) {
            List<DataFusionColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            DataFusionTable t = new DataFusionTable(tableName, databaseColumns, isView);
            for (DataFusionColumn c : databaseColumns) {
                c.setTable(t);
            }

            databaseTables.add(t);
        }

        return new DataFusionSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select table_name " + "from information_schema.tables "
                    + "where table_schema='public'" + "order by table_name; ")) {
                while (rs.next()) {
                    tableNames.add(rs.getString(1));
                }
            }
        }
        return tableNames;
    }

    private static List<DataFusionColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<DataFusionColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    String.format("select * from information_schema.columns where table_name = '%s';", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    boolean isNullable = rs.getString("is_nullable").contentEquals("YES");

                    DataFusionColumn c = new DataFusionColumn(columnName,
                            DataFusionDataType.parseFromDataFusionCatalog(dataType), isNullable);
                    columns.add(c);
                }
            }
        }

        return columns;
    }

    /*
     * When adding a new type: 1. Update all methods inside this enum 2. Update all `DataFusionBaseExpr`'s signature, if
     * it can support new type (in `DataFusionBaseExprFactory.java`
     *
     * Types are 'SQL DataType' in DataFusion's documentation
     * https://datafusion.apache.org/user-guide/sql/data_types.html
     */
    public enum DataFusionDataType {

        BIGINT, DOUBLE, BOOLEAN, NULL;

        public static DataFusionDataType getRandomWithoutNull() {
            DataFusionDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == DataFusionDataType.NULL);
            return dt;
        }

        // How to parse type in DataFusion's catalog to `DataFusionDataType`
        // As displayed in:
        // create table t1(v1 int, v2 bigint);
        // select table_name, column_name, data_type from information_schema.columns;
        public static DataFusionDataType parseFromDataFusionCatalog(String typeString) {
            switch (typeString) {
            case "Int64":
                return DataFusionDataType.BIGINT;
            case "Float64":
                return DataFusionDataType.DOUBLE;
            case "Boolean":
                return DataFusionDataType.BOOLEAN;
            default:
                dfAssert(false, "Unreachable. All branches should be eovered");
            }

            dfAssert(false, "Unreachable. All branches should be eovered");
            return null;
        }

        // TODO(datafusion) lots of hack here, should build our own Randomly later
        public Node<DataFusionExpression> getRandomConstant(DataFusionGlobalState state) {
            if (Randomly.getBooleanWithSmallProbability()) {
                return DataFusionConstant.createNullConstant();
            }
            switch (this) {
            case BIGINT:
                return DataFusionConstant.createIntConstant(state.getRandomly().getInteger());
            case BOOLEAN:
                return new DataFusionConstant.DataFusionBooleanConstant(Randomly.getBoolean());
            case DOUBLE:
                if (Randomly.getBoolean()) {
                    if (Randomly.getBoolean()) {
                        Double randomDouble = state.getRandomly().getDouble(); // [0.0, 1.0);
                        Double scaledDouble = (randomDouble - 0.5) * 2 * Double.MAX_VALUE;
                        return new DataFusionConstant.DataFusionDoubleConstant(scaledDouble);
                    }
                    String doubleStr = Randomly.fromOptions("'NaN'::Double", "'+Inf'::Double", "'-Inf'::Double", "-0.0",
                            "+0.0");
                    return new DataFusionConstant.DataFusionDoubleConstant(doubleStr);
                }

                return new DataFusionConstant.DataFusionDoubleConstant(state.getRandomly().getDouble());
            case NULL:
                return DataFusionConstant.createNullConstant();
            default:
                dfAssert(false, "Unreachable. All branches should be eovered");
            }

            dfAssert(false, "Unreachable. All branches should be eovered");
            return DataFusionConstant.createNullConstant();
        }
    }

    public static class DataFusionColumn extends AbstractTableColumn<DataFusionTable, DataFusionDataType> {

        private final boolean isNullable;

        public DataFusionColumn(String name, DataFusionDataType columnType, boolean isNullable) {
            super(name, null, columnType);
            this.isNullable = isNullable;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class DataFusionTable
            extends AbstractRelationalTable<DataFusionColumn, TableIndex, DataFusionGlobalState> {

        public DataFusionTable(String tableName, List<DataFusionColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

        public static List<DataFusionColumn> getAllColumns(List<DataFusionTable> tables) {
            return tables.stream().map(AbstractTable::getColumns).flatMap(List::stream).collect(Collectors.toList());
        }

        public static List<DataFusionColumn> getRandomColumns(List<DataFusionTable> tables) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                return Arrays.asList(new DataFusionColumn("*", DataFusionDataType.NULL, true));
            }

            List<DataFusionColumn> allColumns = getAllColumns(tables);

            return Randomly.nonEmptySubset(allColumns);
        }
    }

}
