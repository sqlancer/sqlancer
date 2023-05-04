package sqlancer.databend.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendSchema;

public class DatabendInOperation extends NewInOperatorNode<DatabendExpression> implements DatabendExpression {

    private final DatabendExpression leftExpr;
    private final List<DatabendExpression> rightExpr;

    public DatabendInOperation(DatabendExpression left, List<DatabendExpression> right, boolean isNegated) {
        super(DatabendExprToNode.cast(left), DatabendExprToNode.casts(right), isNegated);
        this.leftExpr = left;
        this.rightExpr = right;
    }

    @Override
    public DatabendSchema.DatabendDataType getExpectedType() {
        return DatabendSchema.DatabendDataType.BOOLEAN;
    }

    @Override
    public DatabendConstant getExpectedValue() {
        DatabendConstant leftValue = leftExpr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return DatabendConstant.createNullConstant();
        }
        boolean isNull = false;
        for (DatabendExpression expr : rightExpr) {
            DatabendConstant rightValue = expr.getExpectedValue();
            if (rightValue == null) {
                return null;
            }
            if (rightValue.isNull()) {
                isNull = true;
            } else if (rightValue.isEquals(leftValue).isBoolean() && rightValue.isEquals(leftValue).asBoolean()) {
                return DatabendConstant.createBooleanConstant(!isNegated());
            }
        }

        if (isNull) {
            return DatabendConstant.createNullConstant();
        } else {
            return DatabendConstant.createBooleanConstant(isNegated());
        }
    }
}
