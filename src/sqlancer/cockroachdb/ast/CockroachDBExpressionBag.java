package sqlancer.cockroachdb.ast;

public class CockroachDBExpressionBag implements CockroachDBExpression {
    private CockroachDBExpression innerExpr;

    public CockroachDBExpressionBag(CockroachDBExpression innerExpr) {
        this.innerExpr = innerExpr;
    }

    public void updateInnerExpr(CockroachDBExpression innerExpr) {
        this.innerExpr = innerExpr;
    }

    public CockroachDBExpression getInnerExpr() {
        return innerExpr;
    }

}
