package sqlancer.oxla.ast;

import sqlancer.oxla.schema.OxlaDataType;

public class OxlaCast implements OxlaExpression {
    public final OxlaExpression expression;
    public final OxlaDataType dataType;

    public OxlaCast(OxlaExpression expression, OxlaDataType dataType) {
        if (expression == null) {
            throw new AssertionError("expression was null.");
        }
        this.expression = expression;
        this.dataType = dataType;
    }

    @Override
    public OxlaConstant getExpectedValue() {
        return expression.getExpectedValue().tryCast(dataType);
    }
}
