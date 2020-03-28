package sqlancer.postgres.ast;

import java.util.List;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresInOperation implements PostgresExpression {

	private final PostgresExpression expr;
	private final List<PostgresExpression> listElements;
	private final boolean isTrue;

	public PostgresInOperation(PostgresExpression expr, List<PostgresExpression> listElements, boolean isTrue) {
		this.expr = expr;
		this.listElements = listElements;
		this.isTrue = isTrue;
	}

	public PostgresExpression getExpr() {
		return expr;
	}

	public List<PostgresExpression> getListElements() {
		return listElements;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		if (expr.getExpectedValue().isNull()) {
			return PostgresConstant.createNullConstant();
		}
		boolean isNull = false;
		for (PostgresExpression expr : getListElements()) {
			if (expr.getExpectedValue().isNull()) {
				isNull = true;
			} else if (expr.getExpectedValue().isEquals(this.expr.getExpectedValue()).isBoolean()
					&& expr.getExpectedValue().isEquals(this.expr.getExpectedValue()).asBoolean()) {
				return PostgresConstant.createBooleanConstant(isTrue);
			}
		}

		if (isNull) {
			return PostgresConstant.createNullConstant();
		} else {
			return PostgresConstant.createBooleanConstant(!isTrue);
		}
	}

	public boolean isTrue() {
		return isTrue;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.BOOLEAN;
	}
}
