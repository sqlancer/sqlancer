package sqlancer.mariadb.ast;

public class MariaDBAggregate extends MariaDBExpression {

    private final MariaDBExpression expr;
    private final MariaDBAggregateFunction aggr;

    public MariaDBAggregate(MariaDBExpression expr, MariaDBAggregateFunction aggr) {
        this.expr = expr;
        this.aggr = aggr;
    }

    public enum MariaDBAggregateFunction {
        COUNT
    }

    public MariaDBExpression getExpr() {
        return expr;
    }

    public MariaDBAggregateFunction getAggr() {
        return aggr;
    }

}
