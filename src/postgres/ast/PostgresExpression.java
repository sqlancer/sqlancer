package postgres.ast;

import postgres.PostgresSchema.PostgresDataType;

public abstract class PostgresExpression {
	
	public abstract PostgresDataType getExpressionType();

	public abstract PostgresConstant getExpectedValue();
}
