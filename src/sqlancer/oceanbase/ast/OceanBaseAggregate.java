package sqlancer.oceanbase.ast;

public class OceanBaseAggregate implements OceanBaseExpression {

    private final OceanBaseExpression expr;
    private final OceanBaseAggregateFunction aggr;

    public OceanBaseAggregate(OceanBaseExpression expr, OceanBaseAggregateFunction aggr) {
        this.expr = expr;
        this.aggr = aggr;
    }

    public enum OceanBaseAggregateFunction {
        COUNT
    }

    public OceanBaseExpression getExpr() {
        return expr;
    }

    public OceanBaseAggregateFunction getAggr() {
        return aggr;
    }

}
