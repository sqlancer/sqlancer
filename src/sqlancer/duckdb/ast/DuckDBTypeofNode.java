package sqlancer.duckdb.ast;

public class DuckDBTypeofNode implements DuckDBExpression {

    private final DuckDBExpression expr;

    public DuckDBTypeofNode(DuckDBExpression e) {
        this.expr = e;
    }

    public DuckDBExpression getExpr() {
        return expr;
    }
}
