package lama.mysql.ast;

import lama.Randomly;
import lama.mysql.ast.MySQLCastOperation.CastType;

public class MySQLBinaryOperation extends MySQLExpression {

	private final MySQLExpression left;
	private final MySQLExpression right;
	private final MySQLBinaryOperator op;

	public enum MySQLBinaryOperator {

		AND("&") {
			@Override
			public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
				if (left.isNull() || right.isNull()) {
					return MySQLConstant.createNullConstant();
				} else {
					long leftVal = left.castAs(CastType.SIGNED).getInt();
					long rightVal = right.castAs(CastType.SIGNED).getInt();
					return MySQLConstant.createIntConstant(leftVal & rightVal);
				}
			}
		};

		private String textRepresentation;

		private MySQLBinaryOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public String getTextRepresentation() {
			return textRepresentation;
		}

		public abstract MySQLConstant apply(MySQLConstant left, MySQLConstant right);

		public static MySQLBinaryOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public MySQLBinaryOperation(MySQLExpression left, MySQLExpression right, MySQLBinaryOperator op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}

	@Override
	public MySQLConstant getExpectedValue() {
		return op.apply(left.getExpectedValue(), right.getExpectedValue());
	}

	public MySQLExpression getLeft() {
		return left;
	}

	public MySQLBinaryOperator getOp() {
		return op;
	}

	public MySQLExpression getRight() {
		return right;
	}

}
