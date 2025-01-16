package sqlancer.tidb.ast;

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
