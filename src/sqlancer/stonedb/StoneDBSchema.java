package sqlancer.stonedb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.stonedb.ast.StoneDBConstant;
import sqlancer.stonedb.ast.StoneDBExpression;

public class StoneDBSchema extends AbstractSchema<StoneDBProvider.StoneDBGlobalState, StoneDBSchema.StoneDBTable> {

    public enum StoneDBDataType {

        NULL, TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, YEAR, TIME, DATE, DATETIME, TIMESTAMP,
        CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB;

        public static StoneDBDataType getRandomWithoutNull() {
            List<StoneDBDataType> collect = Stream.of(values()).filter(c -> c != NULL).collect(Collectors.toList());
            return Randomly.fromList(collect);
        }

        public static StoneDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static Node<StoneDBExpression> getRandomValue(StoneDBDataType dataType) {
            switch (dataType) {
            case TINYINT:
                break;
            case SMALLINT:
                break;
            case MEDIUMINT:
                break;
            case INT:
                return StoneDBConstant.createIntConstant((int) Randomly.getNonCachedInteger());
            case BIGINT:
                break;
            case FLOAT:
                break;
            case DOUBLE:
                return StoneDBConstant.createDoubleConstant(Randomly.getUncachedDouble());
            case DECIMAL:
                break;
            case YEAR:
                break;
            case TIME:
                break;
            case DATE:
                return StoneDBConstant.createDateConstant(Randomly.getNonCachedInteger());
            case DATETIME:
                break;
            case TIMESTAMP:
                return StoneDBConstant.createTimestampConstant(Randomly.getNonCachedInteger());
            case CHAR:
                break;
            case VARCHAR:
                break;
            case TINYTEXT:
                break;
            case TEXT:
                return StoneDBConstant.createTextConstant(new Randomly().getString());
            case MEDIUMTEXT:
                break;
            case LONGTEXT:
                break;
            case BINARY:
                break;
            case VARBINARY:
                break;
            case TINYBLOB:
                break;
            case BLOB:
                break;
            case MEDIUMBLOB:
                break;
            case LONGBLOB:
                break;
            default:
                throw new AssertionError();
            }
            return null;
        }

        public static StringBuilder appendTypeAndValue(StoneDBDataType dataType, StringBuilder sb, Randomly r) {
            switch (dataType) {
            case TINYINT:
                sb.append("TINYINT");
                // sb.append(r.getInteger(-128, 127));
                break;
            case SMALLINT:
                sb.append("SMALLINT");
                // sb.append(r.getInteger(-32768, 32767));
                break;
            case MEDIUMINT:
                sb.append("MEDIUMINT");
                // sb.append(r.getInteger(-8388608, 8388607));
                break;
            case INT:
                sb.append("INT");
                // sb.append(r.getInteger(-2147483647, 2147483647));
                break;
            case BIGINT:
                sb.append("BIGINT");
                // sb.append(r.getBigInteger(new BigInteger("-9223372036854775806"), new
                // BigInteger("9223372036854775807")));
                break;
            case FLOAT:
                sb.append("FLOAT");
                optionallyAddPrecisionAndScale(sb);
                break;
            case DOUBLE:
                sb.append("DOUBLE");
                optionallyAddPrecisionAndScale(sb);
                break;
            case DECIMAL:
                sb.append("DECIMAL"); // The default value is P(10,0);
                break;
            case YEAR:
                sb.append("YEAR");
                break;
            case TIME:
                sb.append("TIME");
                break;
            case DATE:
                sb.append("DATE");
                break;
            case DATETIME:
                sb.append("DATETIME");
                break;
            case TIMESTAMP:
                sb.append("TIMESTAMP");
                break;
            case CHAR:
                sb.append("CHAR").append(Randomly.fromOptions("", "(" + r.getInteger(0, 255) + ")"));
                break;
            case VARCHAR:
                sb.append("VARCHAR").append("(").append(r.getInteger(0, 65535)).append(")");
                break;
            case TINYTEXT:
                sb.append("TINYTEXT");
                break;
            case TEXT:
                sb.append("TEXT");
                break;
            case MEDIUMTEXT:
                sb.append("MEDIUMTEXT");
                break;
            case LONGTEXT:
                sb.append("LONGTEXT");
                break;
            case BINARY:
                sb.append("BINARY");
                break;
            case VARBINARY:
                sb.append("VARBINARY").append("(").append(r.getInteger(0, 65535)).append(")");
                break;
            case TINYBLOB:
                sb.append("TINYBLOB");
                break;
            case BLOB:
                sb.append("BLOB");
                break;
            case MEDIUMBLOB:
                sb.append("MEDIUMBLOB");
                break;
            case LONGBLOB:
                sb.append("LONGBLOB");
                break;
            default:
                throw new AssertionError();
            }
            return sb;
        }

        private static void optionallyAddPrecisionAndScale(StringBuilder sb) {
            if (Randomly.getBoolean()) {
                sb.append("(");
                // The maximum number of digits (M) for DECIMAL is 65
                long m = Randomly.getNotCachedInteger(1, 65);
                sb.append(m);
                sb.append(", ");
                // The maximum number of supported decimals (D) is 30
                long nCandidate = Randomly.getNotCachedInteger(1, 30);
                // For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'c0').
                long n = Math.min(nCandidate, m);
                sb.append(n);
                sb.append(")");
            }
        }
    }

    public static class StoneDBTable
            extends AbstractRelationalTable<StoneDBColumn, StoneDBIndex, StoneDBProvider.StoneDBGlobalState> {

        public StoneDBTable(String tableName, List<StoneDBColumn> columns, List<StoneDBIndex> indexes, boolean isView) {
            super(tableName, columns, indexes, isView);
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static class StoneDBTables extends AbstractTables<StoneDBTable, StoneDBColumn> {
        public StoneDBTables(List<StoneDBTable> tables) {
            super(tables);
        }
    }

    public static final class StoneDBIndex extends TableIndex {
        private StoneDBIndex(String indexName) {
            super(indexName);
        }

        public static StoneDBIndex create(String indexName) {
            return new StoneDBIndex(indexName);
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

    public static StoneDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<StoneDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con, databaseName);
        for (String tableName : tableNames) {
            List<StoneDBColumn> databaseColumns = getTableColumns(con, databaseName, tableName);
            List<StoneDBIndex> indexes = getIndexes(con, databaseName, tableName);
            boolean isView = tableName.startsWith("v");
            StoneDBTable t = new StoneDBTable(tableName, databaseColumns, indexes, isView);
            for (StoneDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new StoneDBSchema(databaseTables);
    }

    private static List<StoneDBIndex> getIndexes(SQLConnection con, String databaseName, String tableName)
            throws SQLException {
        List<StoneDBIndex> indexes = new ArrayList<>();
        try (ResultSet rs = con.createStatement()
                .executeQuery("SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '"
                        + databaseName + "' AND TABLE_NAME='" + tableName + "';")) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                indexes.add(StoneDBIndex.create(indexName));
            }
        }
        return indexes;
    }

    private static List<StoneDBColumn> getTableColumns(SQLConnection con, String databaseName, String tableName)
            throws SQLException {
        List<StoneDBColumn> columns = new ArrayList<>();
        try (ResultSet rs = con.createStatement()
                .executeQuery("select * from information_schema.columns where table_schema = '" + databaseName
                        + "' AND TABLE_NAME='" + tableName + "';")) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String dataType = rs.getString("DATA_TYPE");
                int precision = rs.getInt("NUMERIC_PRECISION");
                boolean isNullable = !rs.getString("IS_NULLABLE").equals("NO");
                boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                StoneDBColumn c = new StoneDBColumn(columnName, getColumnCompositeDataType(dataType), isPrimaryKey,
                        isNullable, precision);
                columns.add(c);
            }
        }
        return columns;
    }

    private static StoneDBCompositeDataType getColumnCompositeDataType(String typeString) {
        if (Arrays.stream(StoneDBDataType.values()).noneMatch(e -> e.name().equals(typeString.toUpperCase()))) {
            throw new AssertionError(typeString);
        }
        return new StoneDBCompositeDataType(StoneDBDataType.valueOf(typeString.toUpperCase()));
    }

    public static StoneDBDataType getColumnDataType(String typeString) {
        if (Arrays.stream(StoneDBDataType.values()).noneMatch(e -> e.name().equals(typeString.toUpperCase()))) {
            throw new AssertionError(typeString);
        }
        return StoneDBDataType.valueOf(typeString.toUpperCase());
    }

    private static List<String> getTableNames(SQLConnection con, String databaseName) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (ResultSet rs = con.createStatement()
                .executeQuery("select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                        + databaseName + "';")) {
            while (rs.next()) {
                if (rs.getString("ENGINE").equals("TIANMU")) {
                    tableNames.add(rs.getString("TABLE_NAME"));
                }
            }
        }
        return tableNames;
    }

    public static class StoneDBColumn extends AbstractTableColumn<StoneDBTable, StoneDBCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;
        private final int precision;

        public StoneDBColumn(String name, StoneDBCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable,
                int precision) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
            this.precision = precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public int getPrecision() {
            return precision;
        }

    }

    public StoneDBSchema(List<StoneDBTable> databaseTables) {
        super(databaseTables);
    }

    public static class StoneDBCompositeDataType {
        private final StoneDBDataType dataType;
        private final int size;

        public StoneDBCompositeDataType(StoneDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public StoneDBCompositeDataType(StoneDBDataType dataType) {
            this.dataType = dataType;
            int size = -1;
            switch (dataType) {
            case TINYINT:
                size = 1;
                break;
            case SMALLINT:
                size = 2;
                break;
            case MEDIUMINT:
                size = 3;
                break;
            case INT:
                size = 4;
                break;
            case BIGINT:
                size = 8;
                break;
            case FLOAT:
                size = 4;
                break;
            case DOUBLE:
                size = 8;
                break;
            case DECIMAL:
                size = -2;
                break;
            case YEAR:
                size = -2;
                break;
            case TIME:
                size = -2;
                break;
            case DATE:
                size = -2;
                break;
            case DATETIME:
                size = -2;
                break;
            case TIMESTAMP:
                size = -2;
                break;
            case CHAR:
                size = -2;
                break;
            case VARCHAR:
                size = -2;
                break;
            case TINYTEXT:
                size = -2;
                break;
            case TEXT:
                size = -2;
                break;
            case MEDIUMTEXT:
                size = -2;
                break;
            case LONGTEXT:
                size = -2;
                break;
            case BINARY:
                size = -2;
                break;
            case VARBINARY:
                size = -2;
                break;
            case TINYBLOB:
                size = -2;
                break;
            case BLOB:
                size = -2;
                break;
            case MEDIUMBLOB:
                size = -2;
                break;
            case LONGBLOB:
                size = -2;
                break;
            default:
                throw new AssertionError();
            }
            this.size = size;
        }

        public StoneDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static StoneDBCompositeDataType getRandomWithoutNull() {
            StoneDBDataType type = StoneDBDataType.getRandomWithoutNull();
            return new StoneDBCompositeDataType(type);
        }
    }

    public StoneDBTables getRandomTableNonEmptyTables() {
        return new StoneDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }
}
