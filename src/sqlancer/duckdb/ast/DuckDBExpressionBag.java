package sqlancer.duckdb.ast;


// The ExpressionBag is not a built-in SQL feature, 
// but rather a utility class used in CODDTest's oracle construction
// to substitute expressions with their corresponding constant values.
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
