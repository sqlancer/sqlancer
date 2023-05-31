package sqlancer.materialize.ast;

import sqlancer.materialize.MaterializeCompoundDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeCastOperation implements MaterializeExpression {

    private final MaterializeExpression expression;
    private final MaterializeCompoundDataType type;

    public MaterializeCastOperation(MaterializeExpression expression, MaterializeCompoundDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return type.getDataType();
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant expectedValue = expression.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return expectedValue.cast(type.getDataType());
    }

    public MaterializeExpression getExpression() {
        return expression;
    }

    public MaterializeDataType getType() {
        return type.getDataType();
    }

    public MaterializeCompoundDataType getCompoundType() {
        return type;
    }

}
