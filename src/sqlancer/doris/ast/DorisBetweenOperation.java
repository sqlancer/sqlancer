package sqlancer.doris.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.doris.DorisSchema;

public class DorisBetweenOperation extends NewBetweenOperatorNode<DorisExpression> implements DorisExpression {
    public DorisBetweenOperation(DorisExpression left, DorisExpression middle, DorisExpression right, boolean isTrue) {
        super(left, middle, right, isTrue);
    }

    public DorisExpression getLeftExpr() {
        return left;
    }

    public DorisExpression getMiddleExpr() {
        return middle;
    }

    public DorisExpression getRightExpr() {
        return right;
    }

    @Override
    public DorisConstant getExpectedValue() {
        DorisBinaryComparisonOperation leftComparison = new DorisBinaryComparisonOperation(getMiddleExpr(),
                getLeftExpr(), DorisBinaryComparisonOperation.DorisBinaryComparisonOperator.LESS_EQUALS);
        DorisBinaryComparisonOperation rightComparison = new DorisBinaryComparisonOperation(getLeftExpr(),
                getRightExpr(), DorisBinaryComparisonOperation.DorisBinaryComparisonOperator.LESS_EQUALS);
        return new DorisBinaryLogicalOperation(leftComparison, rightComparison,
                DorisBinaryLogicalOperation.DorisBinaryLogicalOperator.AND).getExpectedValue();
    }

    @Override
    public DorisSchema.DorisDataType getExpectedType() {
        return DorisSchema.DorisDataType.BOOLEAN;
    }

}
