package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public abstract class PostgresExpression {
	
	public abstract PostgresDataType getExpressionType();

	public abstract PostgresConstant getExpectedValue();
}
