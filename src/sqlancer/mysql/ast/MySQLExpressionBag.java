package sqlancer.mysql.ast;

public class MySQLExpressionBag implements MySQLExpression {
    private MySQLExpression innerExpr;

    public MySQLExpressionBag(MySQLExpression innerExpr) {
        this.innerExpr = innerExpr;
    }

    public void updateInnerExpr(MySQLExpression innerExpr) {
        this.innerExpr = innerExpr;
    }

    public MySQLExpression getInnerExpr() {
        return innerExpr;
    }
    
    @Override
    public MySQLConstant getExpectedValue() {
        return innerExpr.getExpectedValue();
    }
}
