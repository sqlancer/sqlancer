package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public final class CnosDBBetweenOperation implements CnosDBExpression {

    private final CnosDBExpression expr;
    private final CnosDBExpression left;
    private final CnosDBExpression right;

    public CnosDBBetweenOperation(CnosDBExpression expr, CnosDBExpression left, CnosDBExpression right) {
        this.expr = expr;
        this.left = left;
        this.right = right;
    }

    public CnosDBExpression getExpr() {
        return expr;
    }

    public CnosDBExpression getLeft() {
        return left;
    }

    public CnosDBExpression getRight() {
        return right;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

}
