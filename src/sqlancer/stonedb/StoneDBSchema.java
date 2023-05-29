package sqlancer.stonedb;

import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import sqlancer.common.schema.TableIndex;
import sqlancer.mysql.MySQLSchema;

import java.util.List;

public class StoneDBSchema extends MySQLSchema {

    public StoneDBSchema(List<MySQLTable> databaseTables) {
        super(databaseTables);
    }

    public static final class StoneDBIndex extends MySQLIndex {
        public StoneDBIndex(String indexName) {
            super(indexName);
        }
    }

    public static final class StoneDBTable  extends MySQLTable{

        public

        public StoneDBTable(String tableName, List<MySQLColumn> columns, List<MySQLIndex> indexes, MySQLEngine engine) {
            super(tableName, columns, indexes, engine);
        }
    }


}