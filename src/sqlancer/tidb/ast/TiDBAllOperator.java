package sqlancer.tidb.ast;

import sqlancer.tidb.ast.TiDBBinaryComparisonOperation.TiDBComparisonOperator;

public class TiDBAllOperator implements TiDBExpression {
    private final TiDBExpression leftExpr;
    private final TiDBExpression rightExpr;
    private final TiDBComparisonOperator op;
    

    public TiDBAllOperator(TiDBExpression leftExpr, TiDBExpression rightExpr, TiDBComparisonOperator op) {
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
        this.op = op;
    }

    public TiDBExpression getLeftExpr() {
        return leftExpr;
    }
    public TiDBExpression getRightExpr() {
        return rightExpr;
    }
    public String getOperator() {
        return op.getTextRepresentation();
    }
}
