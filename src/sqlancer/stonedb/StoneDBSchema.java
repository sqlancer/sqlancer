package sqlancer.stonedb;

import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.ast.MySQLConstant;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StoneDBSchema extends MySQLSchema {

    public StoneDBSchema(List<StoneDBTable> databaseTables) {
        super(databaseTables.stream().map(t -> (MySQLTable) t).collect(Collectors.toList()));
    }

    public static class StoneDBColumn extends MySQLColumn {
        public StoneDBColumn(String name, MySQLDataType columnType, boolean isPrimaryKey, int precision) {
            super(name, columnType, isPrimaryKey, precision);
        }
    }

    public static final class StoneDBTable extends MySQLTable {

        public StoneDBTable(String tableName, List<MySQLColumn> columns, List<MySQLIndex> indexes, MySQLEngine engine) {
            super(tableName, columns, indexes, engine);
        }
    }

    public static final class StoneDBIndex extends MySQLIndex {
        public StoneDBIndex(String indexName) {
            super(indexName);
        }
    }

    public static class StoneDBTables extends MySQLTables {
        public StoneDBTables(List<MySQLTable> tables) {
            super(tables);
        }
    }

    public static final class StoneDBRowValue extends MySQLRowValue {
        public StoneDBRowValue(MySQLTables tables, Map<MySQLColumn, MySQLConstant> values) {
            super(tables, values);
        }
    }

}