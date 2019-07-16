package lama.postgres.ast;

import lama.LikeImplementationHelper;
import lama.postgres.PostgresSchema.PostgresDataType;

public class PostgresLikeOperation extends PostgresExpression {
	
	private final PostgresExpression left;
	private final PostgresExpression right;

	public PostgresLikeOperation(PostgresExpression left, PostgresExpression right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.BOOLEAN;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		PostgresConstant leftVal = left.getExpectedValue();
		PostgresConstant rightVal = right.getExpectedValue();
		if (leftVal.isNull() || rightVal.isNull()) {
			return PostgresConstant.createNullConstant();
		} else {
			boolean val = LikeImplementationHelper.match(leftVal.asString(), rightVal.asString(), 0, 0, true);
			return PostgresConstant.createBooleanConstant(val);
		}
	}
	
	public PostgresExpression getLeft() {
		return left;
	}
	
	public PostgresExpression getRight() {
		return right;
	}

}
