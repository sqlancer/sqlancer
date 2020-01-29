package lama.cockroachdb.ast;

public class CockroachDBAggregate extends CockroachDBExpression {
	
	private CockroachDBAggregateFunction func;
	private CockroachDBExpression expr;

	public enum CockroachDBAggregateFunction {
		SUM;
	}
	
	public CockroachDBAggregate(CockroachDBAggregateFunction func, CockroachDBExpression expr) {
		this.func = func;
		this.expr = expr;
	}
	
	public CockroachDBAggregateFunction getFunc() {
		return func;
	}
	
	public CockroachDBExpression getExpr() {
		return expr;
	}

}
