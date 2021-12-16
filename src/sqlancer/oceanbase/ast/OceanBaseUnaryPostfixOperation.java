package sqlancer.oceanbase.ast;

public class OceanBaseUnaryPostfixOperation implements OceanBaseExpression {

    private final OceanBaseExpression expression;
    private final UnaryPostfixOperator operator;
    private boolean negate;

    public enum UnaryPostfixOperator {
        IS_NULL, IS_TRUE, IS_FALSE;
    }

    public OceanBaseUnaryPostfixOperation(OceanBaseExpression expr, UnaryPostfixOperator op, boolean negate) {
        this.expression = expr;
        this.operator = op;
        this.setNegate(negate);
    }

    public OceanBaseExpression getExpression() {
        return expression;
    }

    public UnaryPostfixOperator getOperator() {
        return operator;
    }

    public boolean isNegated() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        boolean val;
        OceanBaseConstant expectedValue = expression.getExpectedValue();
        switch (operator) {
        case IS_NULL:
            val = expectedValue.isNull();
            break;
        case IS_FALSE:
            val = !expectedValue.isNull() && !expectedValue.asBooleanNotNull();
            break;
        case IS_TRUE:
            val = !expectedValue.isNull() && expectedValue.asBooleanNotNull();
            break;
        default:
            throw new AssertionError(operator);
        }
        if (negate) {
            val = !val;
        }
        return OceanBaseConstant.createIntConstant(val ? 1 : 0);
    }

}
