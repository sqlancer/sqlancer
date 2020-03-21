package lama.cockroachdb.ast;

import java.util.List;

import lama.Randomly;

public class CockroachDBAggregate extends CockroachDBExpression {

	private CockroachDBAggregateFunction func;
	private List<CockroachDBExpression> expr;

	public enum CockroachDBAggregateFunction {
		SUM, SUM_INT, AVG, MIN, MAX, COUNT_ROWS, COUNT, SQRDIFF, STDDEV, VARIANCE, XOR_AGG, //
		BIT_AND, BIT_OR, //
		BOOL_AND, BOOL_OR //
		;
		public static CockroachDBAggregateFunction getRandom() {
			return Randomly.fromOptions(values());
		}

		public static CockroachDBAggregateFunction getRandomMetamorphicOracle() {
			// not: VARIANCE, STDDEV, SQRDIFF
			return Randomly.fromOptions(SUM, SUM_INT, MIN, MAX, XOR_AGG, BIT_AND, BIT_OR, BOOL_AND, BOOL_OR, COUNT, AVG, COUNT_ROWS);
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
