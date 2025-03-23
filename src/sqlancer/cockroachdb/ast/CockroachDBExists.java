package sqlancer.cockroachdb.ast;

public class CockroachDBExists implements CockroachDBExpression {

    private final CockroachDBExpression select;
    private boolean negated = false;

    public CockroachDBExists(CockroachDBExpression select, boolean negated) {
        this.select = select;
        this.negated = negated;
    }

    public void setNegated(boolean negated) {
        this.negated = negated;
    }

    public boolean getNegated() {
        return this.negated;
    }

    public CockroachDBExpression getExpression() {
        return select;
    }
}
