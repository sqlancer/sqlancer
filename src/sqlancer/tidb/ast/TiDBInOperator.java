package sqlancer.tidb.ast;

public class TiDBInOperator implements TiDBExpression {
    private final TiDBExpression left;
    private final TiDBExpression right;

    public TiDBInOperator(TiDBExpression left, TiDBExpression right) {
        this.left = left;
        this.right = right;
    }

    public TiDBExpression getLeft() {
        return left;
    }

    public TiDBExpression getRight() {
        return right;
    }
    
}
