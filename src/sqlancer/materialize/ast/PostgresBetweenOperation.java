package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation.BinaryLogicalOperator;

public final class PostgresBetweenOperation implements PostgresExpression {

    private final PostgresExpression expr;
    private final PostgresExpression left;
    private final PostgresExpression right;
    private final boolean isSymmetric;

    public PostgresBetweenOperation(PostgresExpression expr, PostgresExpression left, PostgresExpression right,
            boolean symmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        isSymmetric = symmetric;
    }

    public PostgresExpression getExpr() {
        return expr;
    }

    public PostgresExpression getLeft() {
        return left;
    }

    public PostgresExpression getRight() {
        return right;
    }

    public boolean isSymmetric() {
        return isSymmetric;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresBinaryComparisonOperation leftComparison = new PostgresBinaryComparisonOperation(left, expr,
                PostgresBinaryComparisonOperator.LESS_EQUALS);
        PostgresBinaryComparisonOperation rightComparison = new PostgresBinaryComparisonOperation(expr, right,
                PostgresBinaryComparisonOperator.LESS_EQUALS);
        PostgresBinaryLogicalOperation andOperation = new PostgresBinaryLogicalOperation(leftComparison,
                rightComparison, PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
        if (isSymmetric) {
            PostgresBinaryComparisonOperation leftComparison2 = new PostgresBinaryComparisonOperation(right, expr,
                    PostgresBinaryComparisonOperator.LESS_EQUALS);
            PostgresBinaryComparisonOperation rightComparison2 = new PostgresBinaryComparisonOperation(expr, left,
                    PostgresBinaryComparisonOperator.LESS_EQUALS);
            PostgresBinaryLogicalOperation andOperation2 = new PostgresBinaryLogicalOperation(leftComparison2,
                    rightComparison2, PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
            PostgresBinaryLogicalOperation orOp = new PostgresBinaryLogicalOperation(andOperation, andOperation2,
                    BinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BOOLEAN;
    }

}
