package sqlancer.mariadb.ast;

import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

public class MariaDBTableReference implements MariaDBExpression {

    private final MariaDBTable table;

    public MariaDBTableReference(MariaDBTable table) {
        this.table = table;
    }

    public MariaDBTable getTable() {
        return table;
    }
}
