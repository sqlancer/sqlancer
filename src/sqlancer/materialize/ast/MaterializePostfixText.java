package sqlancer.materialize.ast;

import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializePostfixText implements MaterializeExpression {

    private final MaterializeExpression expr;
    private final String text;
    private final MaterializeConstant expectedValue;
    private final MaterializeDataType type;

    public MaterializePostfixText(MaterializeExpression expr, String text, MaterializeConstant expectedValue,
            MaterializeDataType type) {
        this.expr = expr;
        this.text = text;
        this.expectedValue = expectedValue;
        this.type = type;
    }

    public MaterializeExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        return expectedValue;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return type;
    }
}
