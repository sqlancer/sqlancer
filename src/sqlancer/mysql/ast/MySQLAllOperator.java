package sqlancer.mysql.ast;

import sqlancer.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;

public class MySQLAllOperator implements MySQLExpression {
    private final MySQLExpression leftExpr;
    private final MySQLExpression rightExpr;
    private final BinaryComparisonOperator op;
    

    public MySQLAllOperator(MySQLExpression leftExpr, MySQLExpression rightExpr, BinaryComparisonOperator op) {
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
        this.op = op;
    }

    public MySQLExpression getLeftExpr() {
        return leftExpr;
    }
    public MySQLExpression getRightExpr() {
        return rightExpr;
    }
    public String getOperator() {
        return op.getTextRepresentation();
    }
}
