package postgres.ast;

import postgres.PostgresSchema.PostgresDataType;

public class PostgresCastOperation extends PostgresExpression {
	
	private PostgresExpression expression;
	private PostgresDataType type;

	public PostgresCastOperation(PostgresExpression expression, PostgresDataType type) {
		this.expression = expression;
		this.type = type;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return type;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		return expression.getExpectedValue().cast(type);
	}

	public PostgresExpression getExpression() {
		return expression;
	}

	public PostgresDataType getType() {
		return type;
	}
	
}
