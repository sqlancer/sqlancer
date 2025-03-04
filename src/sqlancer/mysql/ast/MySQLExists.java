package sqlancer.mysql.ast;

public class MySQLExists implements MySQLExpression {

    private final MySQLExpression expr;
    private final MySQLConstant expected;
    private boolean negated = false;

    public MySQLExists(MySQLExpression expr, MySQLConstant expectedValue) {
        this.expr = expr;
        this.expected = expectedValue;
    }

    public MySQLExists(MySQLExpression expr) {
        this.expr = expr;
        this.expected = expr.getExpectedValue();
        if (expected == null) {
            throw new AssertionError();
        }
    }

    public MySQLExists(MySQLExpression expr, boolean isNegated) {
        this.expr = expr;
        negated = isNegated;
        this.expected = expr.getExpectedValue();
        if (expected == null) {
            throw new AssertionError();
        }
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public boolean isNegated() {
        return negated;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return expected;
    }

}
