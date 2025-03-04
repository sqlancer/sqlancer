package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLTable;

public class MySQLTableAndColumnReference implements MySQLExpression {
    private final MySQLTable table;

    public MySQLTableAndColumnReference(MySQLTable table) {
        this.table = table;
    }

    public MySQLTable getTable() {
        return table;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return MySQLConstant.createNullConstant();
    }
}
