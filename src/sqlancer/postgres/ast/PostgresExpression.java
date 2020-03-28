package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public interface PostgresExpression {
	
	public default PostgresDataType getExpressionType() {
		throw new AssertionError("operator does not support PQS evaluation!");
	}

	public default PostgresConstant getExpectedValue() {
		throw new AssertionError("operator does not support PQS evaluation!");
	}
}
