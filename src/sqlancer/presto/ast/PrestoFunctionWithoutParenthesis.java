package sqlancer.presto.ast;

import sqlancer.presto.PrestoSchema;

public class PrestoFunctionWithoutParenthesis implements PrestoExpression {

    private final PrestoSchema.PrestoCompositeDataType type;
    private final String expr;

    public PrestoFunctionWithoutParenthesis(String expr, PrestoSchema.PrestoCompositeDataType type) {
        this.expr = expr;
        this.type = type;
    }

    public String getExpr() {
        return expr;
    }

    public PrestoSchema.PrestoCompositeDataType getType() {
        return type;
    }

}
