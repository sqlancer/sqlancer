package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

/**
 * @see https://www.sqlite.org/lang_aggfunc.html
 */
public class PostgresAggregate extends PostgresExpression {

	private PostgresAggregateFunction func;
	private PostgresExpression expr;

	public enum PostgresAggregateFunction {
		COUNT;

		public static PostgresAggregateFunction getRandom() {
			return Randomly.fromOptions(values());
		}

		public static PostgresAggregateFunction getRandom(PostgresDataType type) {
			return Randomly.fromOptions(values());
		}

	}

	public PostgresAggregate(PostgresExpression expr, PostgresAggregateFunction func) {
		this.expr = expr;
		this.func = func;
	}

	public PostgresAggregateFunction getFunc() {
		return func;
	}

	public PostgresExpression getExpr() {
		return expr;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		throw new AssertionError();
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.INT;
	}

}
