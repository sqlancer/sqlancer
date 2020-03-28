package sqlancer.postgres.ast;

import java.util.function.BinaryOperator;

import sqlancer.Randomly;
import sqlancer.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresBinaryArithmeticOperation extends PostgresBinaryOperation {

	private final PostgresExpression left;
	private final PostgresExpression right;
	private final PostgresBinaryOperator op;

	public enum PostgresBinaryOperator implements Operator {

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
		// TODO no implementation
		EXPONENTIATION("^") {
			@Override
			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
//				return applyBitOperation(left, right, (l, r) -> (long) Math.pow(l, r));
				throw new AssertionError();
			}
		}

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


	public PostgresBinaryOperator getOp() {
		return op;
	}

	@Override
	public PostgresExpression getLeft() {
		return left;
	}

	@Override
	public PostgresExpression getRight() {
		return right;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.INT;
	}

	@Override
	public String getOperatorTextRepresentation() {
		return op.getTextRepresentation();
	}

}
