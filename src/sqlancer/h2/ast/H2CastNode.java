package sqlancer.h2.ast;

import sqlancer.h2.H2Schema.H2CompositeDataType;

public class H2CastNode implements H2Expression {

    private final H2Expression expression;
    private final H2CompositeDataType type;

    public H2CastNode(H2Expression expression, H2CompositeDataType type) {
        this.expression = expression;
        this.type = type;
    }

    public H2Expression getExpression() {
        return expression;
    }

    public H2CompositeDataType getType() {
        return type;
    }

}
