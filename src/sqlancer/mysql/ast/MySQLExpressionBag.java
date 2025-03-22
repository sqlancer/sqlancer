package sqlancer.mysql.ast;



// The ExpressionBag is not a built-in SQL feature, 
// but rather a utility class used in CODDTest's oracle construction
// to substitute expressions with their corresponding constant values.
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
