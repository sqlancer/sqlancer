package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public interface PostgresExpression {
	
	public PostgresDataType getExpressionType();

	public PostgresConstant getExpectedValue();
}
