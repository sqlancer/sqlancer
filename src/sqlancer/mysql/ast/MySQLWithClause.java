package sqlancer.mysql.ast;

public class MySQLWithClause implements MySQLExpression {
    private MySQLExpression left;
    private MySQLExpression right;

    public MySQLWithClause(MySQLExpression left, MySQLExpression right) {
        this.left = left;
        this.right = right;
    }

    public MySQLExpression getLeft() {
        return this.left;
    }

    public MySQLExpression getRight() {
        return this.right;
    }

    public void updateRight(MySQLExpression right) {
        this.right = right;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return MySQLConstant.createNullConstant();
    }
    
}
