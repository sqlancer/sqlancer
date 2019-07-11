package postgres.ast;

import postgres.PostgresSchema.PostgresDataType;

public class PostgresConcatOperation extends PostgresExpression {

	private final PostgresExpression left;
	private final PostgresExpression right;

	public PostgresConcatOperation(PostgresExpression left, PostgresExpression right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.TEXT;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		String leftStr = left.getExpectedValue().cast(PostgresDataType.TEXT).getTextRepresentation();
		String rightStr = right.getExpectedValue().cast(PostgresDataType.TEXT).getTextRepresentation();
		return PostgresConstant.createTextConstant(leftStr + rightStr);
	}
	
	public PostgresExpression getLeft() {
		return left;
	}
	
	public PostgresExpression getRight() {
		return right;
	}
	
	
}
