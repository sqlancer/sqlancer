package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public interface PostgresExpression {

	default PostgresDataType getExpressionType() {
		throw new AssertionError("operator does not support PQS evaluation!");
	}

	default PostgresConstant getExpectedValue() {
		throw new AssertionError("operator does not support PQS evaluation!");
	}
}
