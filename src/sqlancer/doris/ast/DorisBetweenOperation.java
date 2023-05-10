package sqlancer.doris.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisBetweenOperation extends NewBetweenOperatorNode<DorisExpression> implements DorisExpression {
    public DorisBetweenOperation(DorisExpression left, DorisExpression middle, DorisExpression right, boolean isTrue) {
        super(DorisExprToNode.cast(left), DorisExprToNode.cast(middle), DorisExprToNode.cast(right), isTrue);
    }

    public DorisExpression getLeftExpr() {
        return (DorisExpression) left;
    }

    public DorisExpression getMiddleExpr() {
        return (DorisExpression) middle;
    }

    public DorisExpression getRightExpr() {
        return (DorisExpression) right;
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
