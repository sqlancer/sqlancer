package postgres.ast;

import lama.IgnoreMeException;
import postgres.PostgresSchema.PostgresDataType;

public class PostgresPrefixOperation extends PostgresExpression {

	public enum PrefixOperator {
		NOT("NOT", PostgresDataType.BOOLEAN) {

			@Override
			public PostgresDataType getExpressionType() {
				return PostgresDataType.BOOLEAN;
			}

			@Override
			protected PostgresConstant getExpectedValue(PostgresConstant expectedValue) {
				if (expectedValue.isNull()) {
					return PostgresConstant.createNullConstant();
				} else {
					return PostgresConstant.createBooleanConstant(!expectedValue.cast(PostgresDataType.BOOLEAN).asBoolean());
				}
			}
		},
		UNARY_PLUS("+", PostgresDataType.INT) {

			@Override
			public PostgresDataType getExpressionType() {
				return PostgresDataType.INT;
			}

			@Override
			protected PostgresConstant getExpectedValue(PostgresConstant expectedValue) {
				return expectedValue;
			}

		},
		UNARY_MINUS("-", PostgresDataType.INT) {

			@Override
			public PostgresDataType getExpressionType() {
				return PostgresDataType.INT;
			}

			@Override
			protected PostgresConstant getExpectedValue(PostgresConstant expectedValue) {
				if (expectedValue.isNull()) {
					// TODO
					throw new IgnoreMeException();
				}
				return PostgresConstant.createIntConstant(-expectedValue.asInt());
			}

		};

		private String textRepresentation;
		private PostgresDataType[] dataTypes;

		PrefixOperator(String textRepresentation, PostgresDataType... dataTypes) {
			this.textRepresentation = textRepresentation;
			this.dataTypes = dataTypes;
		}

		public abstract PostgresDataType getExpressionType();

		protected abstract PostgresConstant getExpectedValue(PostgresConstant expectedValue);

	}

	private final PostgresExpression expr;
	private final PrefixOperator op;

	public PostgresPrefixOperation(PostgresExpression expr, PrefixOperator op) {
		this.expr = expr;
		this.op = op;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return op.getExpressionType();
	}

	@Override
	public PostgresConstant getExpectedValue() {
		return op.getExpectedValue(expr.getExpectedValue());
	}

	public PostgresDataType[] getInputDataTypes() {
		return op.dataTypes;
	}

	public String getTextRepresentation() {
		return op.textRepresentation;
	}

	public PostgresExpression getExpression() {
		return expr;
	}

}
