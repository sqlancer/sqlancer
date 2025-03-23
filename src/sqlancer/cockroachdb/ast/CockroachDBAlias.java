package sqlancer.cockroachdb.ast;

public class CockroachDBAlias implements CockroachDBExpression {

    private final CockroachDBExpression expr;
    private final String alias;

    public CockroachDBAlias(CockroachDBExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    public CockroachDBExpression getExpression() {
        return expr;
    }

    public String getAlias() {
        return alias;
    }
}
