package sqlancer.mysql.ast;

public class MySQLAlias implements MySQLExpression {
    private final MySQLExpression expr;
    private final String alias;

    public MySQLAlias(MySQLExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    public MySQLExpression getExpression() {
        return expr;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return MySQLConstant.createNullConstant();
    }
}
