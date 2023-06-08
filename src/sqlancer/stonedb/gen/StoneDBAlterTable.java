package sqlancer.stonedb.gen;

import sqlancer.mysql.gen.MySQLAlterTable;
import sqlancer.stonedb.StoneDBSchema;

public class StoneDBAlterTable extends MySQLAlterTable {
    private final StoneDBSchema schema;

    public StoneDBAlterTable(StoneDBSchema schema) {
        super(schema);
        this.schema = schema;
    }
}
