package postgres.ast;

import java.util.function.BinaryOperator;

import lama.Randomly;
import postgres.PostgresSchema.PostgresDataType;

public class PostgresBinaryArithmeticOperation extends PostgresExpression {

	private final PostgresExpression left;
	private final PostgresExpression right;
	private final PostgresBinaryOperator op;

	public enum PostgresBinaryOperator {

		ADDITION("+") {
			@Override
			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
				return applyBitOperation(left, right, (l, r) -> l + r);
			}

		},
		SUBTRACTION("-") {
			@Override
			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
				return applyBitOperation(left, right, (l, r) -> l - r);
			}
		},
		MULTIPLICATION("*") {
			@Override
			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
				return applyBitOperation(left, right, (l, r) -> l * r);
			}
		},
		DIVISION("/") {

			@Override
			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
				return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);

			}

		},
		MODULO("%") {
			@Override
			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
				return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);

			}
		},
//		EXPONENTIATION("^") {
//			@Override
//			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
//				return applyBitOperation(left, right, (l, r) -> (long) Math.pow(l, r));
//
//			}
//		}

		;

		private static PostgresConstant applyBitOperation(PostgresConstant left, PostgresConstant right,
				BinaryOperator<Long> op) {
			if (left.isNull() || right.isNull()) {
				return PostgresConstant.createNullConstant();
			} else {
				long leftVal = left.cast(PostgresDataType.INT).asInt();
				long rightVal = right.cast(PostgresDataType.INT).asInt();
				long value = op.apply(leftVal, rightVal);
				return PostgresConstant.createIntConstant(value);
			}
		}

		private String textRepresentation;

		private PostgresBinaryOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public String getTextRepresentation() {
			return textRepresentation;
		}

		public abstract PostgresConstant apply(PostgresConstant left, PostgresConstant right);

		public static PostgresBinaryOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public PostgresBinaryArithmeticOperation(PostgresExpression left, PostgresExpression right,
			PostgresBinaryOperator op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		PostgresConstant leftExpected = left.getExpectedValue();
		PostgresConstant rightExpected = right.getExpectedValue();
		return op.apply(leftExpected, rightExpected);
	}

	public PostgresExpression getLeft() {
		return left;
	}

	public PostgresBinaryOperator getOp() {
		return op;
	}

	public PostgresExpression getRight() {
		return right;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.INT;
	}

}
