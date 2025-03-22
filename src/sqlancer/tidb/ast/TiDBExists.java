package sqlancer.tidb.ast;

public class TiDBExists implements TiDBExpression {
    private final TiDBExpression select;
    private boolean negated = false;

    public TiDBExists(TiDBExpression select, boolean negated) {
        this.select = select;
        this.negated = negated;
    }

    public void setNegated(boolean negated) {
        this.negated = negated;
    }

    public boolean getNegated() {
        return this.negated;
    }

    public TiDBExpression getExpression() {
        return select;
    }
    
}
