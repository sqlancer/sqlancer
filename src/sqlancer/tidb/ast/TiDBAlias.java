package sqlancer.tidb.ast;

public class TiDBAlias implements TiDBExpression {
    private final TiDBExpression expr;
    private final String alias;

    public TiDBAlias(TiDBExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    public TiDBExpression getExpression() {
        return expr;
    }

    public String getAlias() {
        return alias;
    }
}
