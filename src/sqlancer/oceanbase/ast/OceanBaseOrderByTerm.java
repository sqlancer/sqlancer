package sqlancer.oceanbase.ast;

import sqlancer.Randomly;

public class OceanBaseOrderByTerm implements OceanBaseExpression {

    private final OceanBaseOrder order;
    private final OceanBaseExpression expr;

    public enum OceanBaseOrder {
        ASC, DESC;

        public static OceanBaseOrder getRandomOrder() {
            return Randomly.fromOptions(OceanBaseOrder.values());
        }
    }

    public OceanBaseOrderByTerm(OceanBaseExpression expr, OceanBaseOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public OceanBaseOrder getOrder() {
        return order;
    }

    public OceanBaseExpression getExpr() {
        return expr;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        throw new AssertionError(this);
    }

}
