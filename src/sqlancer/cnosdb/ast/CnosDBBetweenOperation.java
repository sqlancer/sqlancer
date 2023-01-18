package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBBinaryComparisonOperation.CnosDBBinaryComparisonOperator;
import sqlancer.cnosdb.ast.CnosDBBinaryLogicalOperation.BinaryLogicalOperator;

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
    public CnosDBConstant getExpectedValue() {
        CnosDBBinaryComparisonOperation leftComparison = new CnosDBBinaryComparisonOperation(left, expr,
                CnosDBBinaryComparisonOperator.LESS_EQUALS);
        CnosDBBinaryComparisonOperation rightComparison = new CnosDBBinaryComparisonOperation(expr, right,
                CnosDBBinaryComparisonOperator.LESS_EQUALS);
        CnosDBBinaryLogicalOperation andOperation = new CnosDBBinaryLogicalOperation(leftComparison, rightComparison,
                BinaryLogicalOperator.AND);
        return andOperation.getExpectedValue();
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

}
