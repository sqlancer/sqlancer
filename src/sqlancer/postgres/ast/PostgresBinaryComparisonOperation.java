package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresBinaryComparisonOperation extends PostgresBinaryOperation {
	
	public enum PostgresBinaryComparisonOperator {
		EQUALS("=") {
			@Override
			public PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal) {
				return leftVal.isEquals(rightVal);
			}
		},
		IS_DISTINCT("IS DISTINCT FROM") {
			@Override
			public PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal) {
				return PostgresConstant.createBooleanConstant(!IS_NOT_DISTINCT.getExpectedValue(leftVal, rightVal).asBoolean());
			}
		},
		IS_NOT_DISTINCT("IS NOT DISTINCT FROM") {
			@Override
			public PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal) {
				if (leftVal.isNull()) {
					return PostgresConstant.createBooleanConstant(rightVal.isNull());
				} else if (rightVal.isNull()) {
					return PostgresConstant.createFalse();
				} else {
					return leftVal.isEquals(rightVal);
				}
			}
		},
		NOT_EQUALS("!=") {
			@Override
			public PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal) {
				PostgresConstant isEquals = leftVal.isEquals(rightVal);
				if (isEquals.isBoolean()) {
					return PostgresConstant.createBooleanConstant(!isEquals.asBoolean());
				}
				return isEquals;
			}
		},
		LESS("<") {

			@Override
			public PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal) {
				return leftVal.isLessThan(rightVal);
			}
		},
		LESS_EQUALS("<=") {

			@Override
			public PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal) {
				PostgresConstant lessThan = leftVal.isLessThan(rightVal);
				if (lessThan.isBoolean() && !lessThan.asBoolean()) {
					return leftVal.isEquals(rightVal);
				} else {
					return lessThan;
				}
			}
		},
		GREATER(">") {
			@Override
			public PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal) {
				PostgresConstant equals = leftVal.isEquals(rightVal);
				if (equals.isBoolean() && equals.asBoolean()) {
					return PostgresConstant.createFalse();
				} else {
					PostgresConstant applyLess = leftVal.isLessThan(rightVal);
					if (applyLess.isNull()) {
						return PostgresConstant.createNullConstant();
					}
					return PostgresPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
				}
			}
		},
		GREATER_EQUALS(">=") {

			@Override
			public PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal) {
				PostgresConstant equals = leftVal.isEquals(rightVal);
				if (equals.isBoolean() && equals.asBoolean()) {
					return PostgresConstant.createTrue();
				} else {
					PostgresConstant applyLess = leftVal.isLessThan(rightVal);
					if (applyLess.isNull()) {
						return PostgresConstant.createNullConstant();
					}
					return PostgresPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
				}
			}

		};
		
		private final String textRepresentation;

		public String getTextRepresentation() {
			return textRepresentation;
		}

		private PostgresBinaryComparisonOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public abstract PostgresConstant getExpectedValue(PostgresConstant leftVal, PostgresConstant rightVal);

		public static PostgresBinaryComparisonOperator getRandom() {
			return Randomly.fromOptions(PostgresBinaryComparisonOperator.values());
		}
		
	}
	
	private final PostgresExpression left;
	private final PostgresExpression right;
	private final PostgresBinaryComparisonOperator op;

	public PostgresBinaryComparisonOperation(PostgresExpression left, PostgresExpression right, PostgresBinaryComparisonOperator op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}

	public PostgresExpression getLeft() {
		return left;
	}

	public PostgresBinaryComparisonOperator getOp() {
		return op;
	}

	public PostgresExpression getRight() {
		return right;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.BOOLEAN;
	}

	@Override
	public String getOperatorTextRepresentation() {
		return op.getTextRepresentation();
	}


}
