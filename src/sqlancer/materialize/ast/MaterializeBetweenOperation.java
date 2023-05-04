package sqlancer.materialize.ast;

import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeBinaryComparisonOperation.MaterializeBinaryComparisonOperator;
import sqlancer.materialize.ast.MaterializeBinaryLogicalOperation.BinaryLogicalOperator;

public final class MaterializeBetweenOperation implements MaterializeExpression {

    private final MaterializeExpression expr;
    private final MaterializeExpression left;
    private final MaterializeExpression right;
    private final boolean isSymmetric;

    public MaterializeBetweenOperation(MaterializeExpression expr, MaterializeExpression left,
            MaterializeExpression right, boolean symmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        isSymmetric = symmetric;
    }

    public MaterializeExpression getExpr() {
        return expr;
    }

    public MaterializeExpression getLeft() {
        return left;
    }

    public MaterializeExpression getRight() {
        return right;
    }

    public boolean isSymmetric() {
        return isSymmetric;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeBinaryComparisonOperation leftComparison = new MaterializeBinaryComparisonOperation(left, expr,
                MaterializeBinaryComparisonOperator.LESS_EQUALS);
        MaterializeBinaryComparisonOperation rightComparison = new MaterializeBinaryComparisonOperation(expr, right,
                MaterializeBinaryComparisonOperator.LESS_EQUALS);
        MaterializeBinaryLogicalOperation andOperation = new MaterializeBinaryLogicalOperation(leftComparison,
                rightComparison, MaterializeBinaryLogicalOperation.BinaryLogicalOperator.AND);
        if (isSymmetric) {
            MaterializeBinaryComparisonOperation leftComparison2 = new MaterializeBinaryComparisonOperation(right, expr,
                    MaterializeBinaryComparisonOperator.LESS_EQUALS);
            MaterializeBinaryComparisonOperation rightComparison2 = new MaterializeBinaryComparisonOperation(expr, left,
                    MaterializeBinaryComparisonOperator.LESS_EQUALS);
            MaterializeBinaryLogicalOperation andOperation2 = new MaterializeBinaryLogicalOperation(leftComparison2,
                    rightComparison2, MaterializeBinaryLogicalOperation.BinaryLogicalOperator.AND);
            MaterializeBinaryLogicalOperation orOp = new MaterializeBinaryLogicalOperation(andOperation, andOperation2,
                    BinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }

}
