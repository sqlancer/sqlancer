package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLColumn;

public class MySQLColumnReference implements MySQLExpression {

    private final MySQLColumn column;
    private final MySQLConstant value;

    public MySQLColumnReference(MySQLColumn column, MySQLConstant value) {
        this.column = column;
        this.value = value;
    }

    public static MySQLColumnReference create(MySQLColumn column, MySQLConstant value) {
        return new MySQLColumnReference(column, value);
    }

    public MySQLColumn getColumn() {
        return column;
    }

    public MySQLConstant getValue() {
        return value;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return value;
    }

}
