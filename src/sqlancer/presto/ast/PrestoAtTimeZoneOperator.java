package sqlancer.presto.ast;

public class PrestoAtTimeZoneOperator implements PrestoExpression {

    private final PrestoExpression expr;
    private final PrestoExpression timeZone;

    public PrestoAtTimeZoneOperator(PrestoExpression expr, PrestoExpression timeZone) {
        this.expr = expr;
        this.timeZone = timeZone;
    }

    public PrestoExpression getExpr() {
        return expr;
    }

    public PrestoExpression getTimeZone() {
        return timeZone;
    }
}
