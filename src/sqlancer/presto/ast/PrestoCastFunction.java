package sqlancer.presto.ast;

import sqlancer.presto.PrestoSchema;

public class PrestoCastFunction implements PrestoExpression {

    private final PrestoExpression expr;
    private final PrestoSchema.PrestoCompositeDataType type;

    public PrestoCastFunction(PrestoExpression expr, PrestoSchema.PrestoCompositeDataType type) {
        this.expr = expr;
        this.type = type;
    }

    public PrestoExpression getExpr() {
        return expr;
    }

    public PrestoSchema.PrestoCompositeDataType getType() {
        return type;
    }

}
