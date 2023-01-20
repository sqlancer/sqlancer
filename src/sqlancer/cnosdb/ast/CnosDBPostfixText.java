package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBPostfixText implements CnosDBExpression {

    private final CnosDBExpression expr;
    private final String text;
    private final CnosDBDataType type;

    public CnosDBPostfixText(CnosDBExpression expr, String text, CnosDBDataType type) {
        this.expr = expr;
        this.text = text;
        this.type = type;
    }

    public CnosDBExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return type;
    }
}
