package sqlancer.tidb.ast;

public class TiDBWithClause implements TiDBExpression {
    private TiDBExpression left;
    private TiDBExpression right;

    public TiDBWithClause(TiDBExpression left, TiDBExpression right) {
        this.left = left;
        this.right = right;
    }

    public TiDBExpression getLeft() {
        return this.left;
    }

    public TiDBExpression getRight() {
        return this.right;
    }

    public void updateRight(TiDBExpression right) {
        this.right = right;
    }
    
}
