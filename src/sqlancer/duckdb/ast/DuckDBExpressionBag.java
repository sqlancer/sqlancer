package sqlancer.duckdb.ast;

public class DuckDBExpressionBag implements DuckDBExpression {
    DuckDBExpression expr;

    public DuckDBExpressionBag(DuckDBExpression e) {
        this.expr = e;
    }

    public void updateInnerExpr(DuckDBExpression expr) {
        this.expr = expr;
    }

    public DuckDBExpression getInnerExpr() {
        return this.expr;
    }
}
