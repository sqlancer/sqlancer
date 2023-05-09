package sqlancer.doris.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisInOperation extends NewInOperatorNode<DorisExpression> implements DorisExpression {

    private final DorisExpression leftExpr;
    private final List<DorisExpression> rightExpr;

    public DorisInOperation(DorisExpression left, List<DorisExpression> right, boolean isNegated) {
        super(DorisExprToNode.cast(left), DorisExprToNode.casts(right), isNegated);
        this.leftExpr = left;
        this.rightExpr = right;
    }

    @Override
    public DorisSchema.DorisDataType getExpectedType() {
        return DorisSchema.DorisDataType.BOOLEAN;
    }

    @Override
    public DorisConstant getExpectedValue() {
        DorisConstant leftValue = leftExpr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return DorisConstant.createNullConstant();
        }
        boolean containNull = false;
        for (DorisExpression expr : rightExpr) {
            DorisConstant rightValue = expr.getExpectedValue();
            if (rightValue == null) {
                return null;
            }
            if (rightValue.isNull()) {
                containNull = true;
            } else if (rightValue.valueEquals(leftValue).isBoolean() && rightValue.valueEquals(leftValue).asBoolean()) {
                return DorisConstant.createBooleanConstant(!isNegated());
            }
        }

        if (containNull) {
            return DorisConstant.createNullConstant();
        }
        // should return false when not considering isNegated op
        return DorisConstant.createBooleanConstant(isNegated());
    }
}
