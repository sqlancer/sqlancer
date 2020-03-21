package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresConcatOperation extends PostgresBinaryOperation {

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
		if (left.getExpectedValue().isNull() || right.getExpectedValue().isNull()) {
			return PostgresConstant.createNullConstant();
		}
		String leftStr = left.getExpectedValue().cast(PostgresDataType.TEXT).getUnquotedTextRepresentation();
		String rightStr = right.getExpectedValue().cast(PostgresDataType.TEXT).getUnquotedTextRepresentation();
		return PostgresConstant.createTextConstant(leftStr + rightStr);
	}
	
	public PostgresExpression getLeft() {
		return left;
	}
	
	public PostgresExpression getRight() {
		return right;
	}

	@Override
	public String getOperatorTextRepresentation() {
		return "||";
	}
	
	
}
