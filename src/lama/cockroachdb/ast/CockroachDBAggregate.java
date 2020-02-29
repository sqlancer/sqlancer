package lama.cockroachdb.ast;

import java.util.List;

import lama.Randomly;

public class CockroachDBAggregate extends CockroachDBExpression {

	private CockroachDBAggregateFunction func;
	private List<CockroachDBExpression> expr;

	public enum CockroachDBAggregateFunction {
		SUM, SUM_INT, AVG, MIN, MAX, COUNT_ROWS, SQRDIFF, STDDEV, VARIANCE, XOR_AGG;
		public static CockroachDBAggregateFunction getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public CockroachDBAggregate(CockroachDBAggregateFunction func, List<CockroachDBExpression> expr) {
		this.func = func;
		this.expr = expr;
	}

	public CockroachDBAggregateFunction getFunc() {
		return func;
	}

	public List<CockroachDBExpression> getExpr() {
		return expr;
	}

}
