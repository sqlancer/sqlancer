package sqlancer.oceanbase.ast;

public class OceanBaseExists implements OceanBaseExpression {

    private final OceanBaseExpression expr;
    private final OceanBaseConstant expected;

    public OceanBaseExists(OceanBaseExpression expr, OceanBaseConstant expectedValue) {
        this.expr = expr;
        this.expected = expectedValue;
    }

    public OceanBaseExists(OceanBaseExpression expr) {
        this.expr = expr;
        this.expected = expr.getExpectedValue();
        if (expected == null) {
            throw new AssertionError();
        }
    }

    public OceanBaseExpression getExpr() {
        return expr;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        return expected;
    }

}
