package sqlancer.tidb.ast;

// The ExpressionBag is not a built-in SQL feature, 
// but rather a utility class used in CODDTest's oracle construction
// to substitute expressions with their corresponding constant values.
public class TiDBExpressionBag implements TiDBExpression {
    private TiDBExpression innerExpr;

    public TiDBExpressionBag(TiDBExpression innerExpr) {
        this.innerExpr = innerExpr;
    }

    public void updateInnerExpr(TiDBExpression innerExpr) {
        this.innerExpr = innerExpr;
    }

    public TiDBExpression getInnerExpr() {
        return innerExpr;
    }
    
}
