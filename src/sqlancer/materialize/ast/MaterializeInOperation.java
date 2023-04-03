package sqlancer.materialize.ast;

import java.util.List;

import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeInOperation implements MaterializeExpression {

    private final MaterializeExpression expr;
    private final List<MaterializeExpression> listElements;
    private final boolean isTrue;

    public MaterializeInOperation(MaterializeExpression expr, List<MaterializeExpression> listElements,
            boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public MaterializeExpression getExpr() {
        return expr;
    }

    public List<MaterializeExpression> getListElements() {
        return listElements;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant leftValue = expr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return MaterializeConstant.createNullConstant();
        }
        boolean isNull = false;
        for (MaterializeExpression expr : getListElements()) {
            MaterializeConstant rightExpectedValue = expr.getExpectedValue();
            if (rightExpectedValue == null) {
                return null;
            }
            if (rightExpectedValue.isNull()) {
                isNull = true;
            } else if (rightExpectedValue.isEquals(this.expr.getExpectedValue()).isBoolean()
                    && rightExpectedValue.isEquals(this.expr.getExpectedValue()).asBoolean()) {
                return MaterializeConstant.createBooleanConstant(isTrue);
            }
        }

        if (isNull) {
            return MaterializeConstant.createNullConstant();
        } else {
            return MaterializeConstant.createBooleanConstant(!isTrue);
        }
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }
}
