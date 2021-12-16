package sqlancer.oceanbase.ast;

public class OceanBaseText implements OceanBaseExpression {

    private final OceanBaseExpression expr;
    private final String text;
    private final boolean prefix;

    public OceanBaseText(OceanBaseExpression expr, String text, boolean prefix) {
        this.expr = expr;
        this.text = text;
        this.prefix = prefix;
    }

    public OceanBaseExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    public boolean isPrefix() {
        return prefix;
    }
}
