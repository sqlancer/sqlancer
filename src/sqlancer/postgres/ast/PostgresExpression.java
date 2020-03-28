package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public interface PostgresExpression {
	
	public PostgresDataType getExpressionType();

	public default PostgresConstant getExpectedValue() {
		throw new AssertionError("operator does not support PQS evaluation!");
	}
}
