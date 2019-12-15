package lama.postgres.ast;

import lama.Randomly;
import lama.postgres.PostgresSchema.PostgresDataType;

public class PostgresBinaryBitOperation extends PostgresBinaryOperation {
	
	private PostgresBinaryBitOperator op;
	private PostgresExpression left;
	private PostgresExpression right;

	public static enum PostgresBinaryBitOperator {
		CONCATENATION("||"), BITWISE_AND("&"), BITWISE_OR("|"), BITWISE_XOR("#"), BITWISE_SHIFT_LEFT("<<"), BITWISE_SHIFT_RIGHT(">>");
		
		private String text;

		private PostgresBinaryBitOperator(String text) {
			this.text = text;
		}

		public String getText() {
			return text;
		}
		
		public static PostgresBinaryBitOperator getRandom() {
			return Randomly.fromOptions(PostgresBinaryBitOperator.values());
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
		return op.getText();
	}

}
