package sqlancer.postgres.ast;

import java.util.Arrays;

import sqlancer.Randomly;
import sqlancer.ast.FunctionNode;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresAggregate.PostgresAggregateFunction;

/**
 * @see https://www.sqlite.org/lang_aggfunc.html
 */
public class PostgresAggregate extends FunctionNode<PostgresAggregateFunction, PostgresExpression>
		implements PostgresExpression {

	public enum PostgresAggregateFunction {
		COUNT;

		public static PostgresAggregateFunction getRandom() {
			return Randomly.fromOptions(values());
		}

		public static PostgresAggregateFunction getRandom(PostgresDataType type) {
			return Randomly.fromOptions(values());
		}

	}

	public PostgresAggregate(PostgresExpression arg, PostgresAggregateFunction func) {
		super(func, Arrays.asList(arg));
	}

}
