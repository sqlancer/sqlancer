package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;

public class PostgresJoin extends PostgresExpression {

	public static enum PostgresJoinType {
		INNER, LEFT, RIGHT, FULL, CROSS;


		public static PostgresJoinType getRandom() {
			return Randomly.fromOptions(values());
		}
		
	}

	private final PostgresTable table;
	private final PostgresExpression onClause;
	private final PostgresJoinType type;

	public PostgresJoin(PostgresTable table, PostgresExpression onClause, PostgresJoinType type) {
		this.table = table;
		this.onClause = onClause;
		this.type = type;
	}

	public PostgresTable getTable() {
		return table;
	}

	public PostgresExpression getOnClause() {
		return onClause;
	}

	public PostgresJoinType getType() {
		return type;
	}

	@Override
	public PostgresDataType getExpressionType() {
		throw new AssertionError();
	}

	@Override
	public PostgresConstant getExpectedValue() {
		throw new AssertionError();
	}


}
