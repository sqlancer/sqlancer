package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresBinaryLogicalOperation extends PostgresBinaryOperation {

	private final PostgresExpression left;
	private final PostgresExpression right;
	private final BinaryLogicalOperator op;

	public enum BinaryLogicalOperator {
		AND {
			@Override
			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
				left = left.cast(PostgresDataType.BOOLEAN);
				right = right.cast(PostgresDataType.BOOLEAN);
				if (left.isNull()) {
					if (right.isNull()) {
						return PostgresConstant.createNullConstant();
					} else {
						if (right.asBoolean()) {
							return PostgresConstant.createNullConstant();
						} else {
							return PostgresConstant.createFalse();
						}
					}
				} else if (!left.asBoolean()) {
					return PostgresConstant.createFalse();
				}
				assert left.asBoolean();
				if (right.isNull()) {
					return PostgresConstant.createNullConstant();
				} else {
					return PostgresConstant.createBooleanConstant(right.isBoolean() && right.asBoolean());
				}
			}
		},
		OR {
			@Override
			public PostgresConstant apply(PostgresConstant left, PostgresConstant right) {
				left = left.cast(PostgresDataType.BOOLEAN);
				right = right.cast(PostgresDataType.BOOLEAN);
				if (left.isBoolean() && left.asBoolean()) {
					return PostgresConstant.createTrue();
				}
				if (right.isBoolean() && right.asBoolean()) {
					return PostgresConstant.createTrue();
				}
				if (left.isNull() || right.isNull()) {
					return PostgresConstant.createNullConstant();
				}
				return PostgresConstant.createFalse();
			}
		};

		public abstract PostgresConstant apply(PostgresConstant left, PostgresConstant right);

		public static BinaryLogicalOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public PostgresBinaryLogicalOperation(PostgresExpression left, PostgresExpression right, BinaryLogicalOperator op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}

	public PostgresExpression getLeft() {
		return left;
	}

	public BinaryLogicalOperator getOp() {
		return op;
	}

	public PostgresExpression getRight() {
		return right;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.BOOLEAN;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		return op.apply(left.getExpectedValue(), right.getExpectedValue());
	}

	@Override
	public String getOperatorTextRepresentation() {
		return op.toString();
	}

}
