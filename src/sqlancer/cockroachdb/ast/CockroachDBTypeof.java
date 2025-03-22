package sqlancer.cockroachdb.ast;

public class CockroachDBTypeof implements CockroachDBExpression {
    private final CockroachDBExpression expr;
    public CockroachDBTypeof(CockroachDBExpression expr) {
        this.expr = expr;
    }
    public CockroachDBExpression getExpr() {
        return expr;
    }
}
