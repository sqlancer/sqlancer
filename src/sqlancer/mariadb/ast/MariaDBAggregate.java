package sqlancer.mariadb.ast;

public class MariaDBAggregate extends MariaDBExpression {
	
	private MariaDBExpression expr;
	private MariaDBAggregateFunction aggr;

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
