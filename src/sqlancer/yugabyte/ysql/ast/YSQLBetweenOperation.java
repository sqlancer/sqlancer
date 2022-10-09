package sqlancer.yugabyte.ysql.ast;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public final class YSQLBetweenOperation implements YSQLExpression {

    private final YSQLExpression expr;
    private final YSQLExpression left;
    private final YSQLExpression right;
    private final boolean isSymmetric;

    public YSQLBetweenOperation(YSQLExpression expr, YSQLExpression left, YSQLExpression right, boolean symmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        isSymmetric = symmetric;
    }

    public YSQLExpression getExpr() {
        return expr;
    }

    public YSQLExpression getLeft() {
        return left;
    }

    public YSQLExpression getRight() {
        return right;
    }

    public boolean isSymmetric() {
        return isSymmetric;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.BOOLEAN;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        YSQLBinaryComparisonOperation leftComparison = new YSQLBinaryComparisonOperation(left, expr,
                YSQLBinaryComparisonOperation.YSQLBinaryComparisonOperator.LESS_EQUALS);
        YSQLBinaryComparisonOperation rightComparison = new YSQLBinaryComparisonOperation(expr, right,
                YSQLBinaryComparisonOperation.YSQLBinaryComparisonOperator.LESS_EQUALS);
        YSQLBinaryLogicalOperation andOperation = new YSQLBinaryLogicalOperation(leftComparison, rightComparison,
                YSQLBinaryLogicalOperation.BinaryLogicalOperator.AND);
        if (isSymmetric) {
            YSQLBinaryComparisonOperation leftComparison2 = new YSQLBinaryComparisonOperation(right, expr,
                    YSQLBinaryComparisonOperation.YSQLBinaryComparisonOperator.LESS_EQUALS);
            YSQLBinaryComparisonOperation rightComparison2 = new YSQLBinaryComparisonOperation(expr, left,
                    YSQLBinaryComparisonOperation.YSQLBinaryComparisonOperator.LESS_EQUALS);
            YSQLBinaryLogicalOperation andOperation2 = new YSQLBinaryLogicalOperation(leftComparison2, rightComparison2,
                    YSQLBinaryLogicalOperation.BinaryLogicalOperator.AND);
            YSQLBinaryLogicalOperation orOp = new YSQLBinaryLogicalOperation(andOperation, andOperation2,
                    YSQLBinaryLogicalOperation.BinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

}
