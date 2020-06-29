package sqlancer.mariadb.ast;

import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;

public class MariaDBColumnName extends MariaDBExpression {

    private final MariaDBColumn column;

    public MariaDBColumnName(MariaDBColumn column) {
        this.column = column;
    }

    public MariaDBColumn getColumn() {
        return column;
    }

}
