package sqlancer.cockroachdb.ast;

import sqlancer.cockroachdb.ast.CockroachDBBinaryComparisonOperator.CockroachDBComparisonOperator;

public class CockroachDBAnyOperator implements CockroachDBExpression {
    private final CockroachDBExpression leftExpr;
    private final CockroachDBExpression rightExpr;
    private final CockroachDBComparisonOperator op;
    

    public CockroachDBAnyOperator(CockroachDBExpression leftExpr, CockroachDBExpression rightExpr, CockroachDBComparisonOperator op) {
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
        this.op = op;
    }

    public CockroachDBExpression getLeftExpr() {
        return leftExpr;
    }
    public CockroachDBExpression getRightExpr() {
        return rightExpr;
    }
    public String getOperator() {
        return op.getTextRepresentation();
    }
}
