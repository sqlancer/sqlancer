package lama.postgres.ast;

import lama.postgres.PostgresSchema.PostgresDataType;

public abstract class PostgresExpression {
	
	public abstract PostgresDataType getExpressionType();

	public abstract PostgresConstant getExpectedValue();
}
