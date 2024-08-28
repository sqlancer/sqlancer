package sqlancer.databend.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.databend.DatabendSchema;

public class DatabendBetweenOperation extends NewBetweenOperatorNode<DatabendExpression> implements DatabendExpression {
    public DatabendBetweenOperation(DatabendExpression left, DatabendExpression middle, DatabendExpression right,
            boolean isTrue) {
        super(left, middle, right, isTrue);
    }

    public DatabendExpression getLeftExpr() {
        return left;
    }

    public DatabendExpression getMiddleExpr() {
        return middle;
    }

    public DatabendExpression getRightExpr() {
        return right;
    }

    @Override
    public DatabendConstant getExpectedValue() {
        DatabendBinaryComparisonOperation leftComparison = new DatabendBinaryComparisonOperation(getMiddleExpr(),
                getLeftExpr(), DatabendBinaryComparisonOperation.DatabendBinaryComparisonOperator.LESS_EQUALS);
        DatabendBinaryComparisonOperation rightComparison = new DatabendBinaryComparisonOperation(getLeftExpr(),
                getRightExpr(), DatabendBinaryComparisonOperation.DatabendBinaryComparisonOperator.LESS_EQUALS);
        return new DatabendBinaryLogicalOperation(leftComparison, rightComparison,
                DatabendBinaryLogicalOperation.DatabendBinaryLogicalOperator.AND).getExpectedValue();
    }

    @Override
    public DatabendSchema.DatabendDataType getExpectedType() {
        return DatabendSchema.DatabendDataType.BOOLEAN;
    }

}
