package sqlancer.cockroachdb.ast;

public class CockroachDBWithClasure implements CockroachDBExpression {

    private CockroachDBExpression left;
    private CockroachDBExpression right;

    public CockroachDBWithClasure(CockroachDBExpression left, CockroachDBExpression right) {
        this.left = left;
        this.right = right;
    }

    public CockroachDBExpression getLeft() {
        return this.left;
    }

    public CockroachDBExpression getRight() {
        return this.right;
    }

    public void updateRight(CockroachDBExpression right) {
        this.right = right;
    }
}
