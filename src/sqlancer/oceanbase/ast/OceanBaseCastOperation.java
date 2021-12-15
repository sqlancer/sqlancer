package sqlancer.oceanbase.ast;

public class OceanBaseCastOperation implements OceanBaseExpression {

    private final OceanBaseExpression expr;
    private final CastType type;

    public enum CastType {
        SIGNED, UNSIGNED;

        public static CastType getRandom() {
            return SIGNED;
            // return Randomly.fromOptions(CastType.values());
        }

    }

    public OceanBaseCastOperation(OceanBaseExpression expr, CastType type) {
        this.expr = expr;
        this.type = type;
    }

    public OceanBaseExpression getExpr() {
        return expr;
    }

    public CastType getType() {
        return type;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(type);
    }

}
