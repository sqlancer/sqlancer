package sqlancer.cockroachdb.ast;


// The ExpressionBag is not a built-in SQL feature, 
// but rather a utility class used in CODDTest's oracle construction
// to substitute expressions with their corresponding constant values.
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
