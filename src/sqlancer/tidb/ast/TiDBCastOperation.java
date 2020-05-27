package sqlancer.tidb.ast;

public class TiDBCastOperation implements TiDBExpression {

    private final TiDBExpression expr;
    private final String type;

    public TiDBCastOperation(TiDBExpression expr, String type) {
        this.expr = expr;
        this.type = type;
    }

    public TiDBExpression getExpr() {
        return expr;
    }

    public String getType() {
        return type;
    }

}
