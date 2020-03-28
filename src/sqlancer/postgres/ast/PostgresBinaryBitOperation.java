package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresBinaryBitOperation extends PostgresBinaryOperation {

	private PostgresBinaryBitOperator op;
	private PostgresExpression left;
	private PostgresExpression right;

	public static enum PostgresBinaryBitOperator implements Operator {
		CONCATENATION("||"), BITWISE_AND("&"), BITWISE_OR("|"), BITWISE_XOR("#"), BITWISE_SHIFT_LEFT("<<"),
		BITWISE_SHIFT_RIGHT(">>");

		private String text;

		private PostgresBinaryBitOperator(String text) {
			this.text = text;
		}

		public static PostgresBinaryBitOperator getRandom() {
			return Randomly.fromOptions(PostgresBinaryBitOperator.values());
		}

		@Override
		public String getTextRepresentation() {
			return text;
		}

	}

	public PostgresBinaryBitOperation(PostgresBinaryBitOperator op, PostgresExpression left, PostgresExpression right) {
		this.op = op;
		this.left = left;
		this.right = right;
	}

	public PostgresExpression getLeft() {
		return left;
	}

	public PostgresExpression getRight() {
		return right;
	}

	public PostgresBinaryBitOperator getOp() {
		return op;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		throw new AssertionError();
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.BIT;
	}

	@Override
	public String getOperatorTextRepresentation() {
		return op.getTextRepresentation();
	}

}
