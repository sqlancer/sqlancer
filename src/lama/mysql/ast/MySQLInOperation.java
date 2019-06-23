package lama.mysql.ast;

import java.util.List;

/**
 * @see https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_in
 */
public class MySQLInOperation extends MySQLExpression {

	private final MySQLExpression expr;
	private final List<MySQLExpression> listElements;
	private final boolean isTrue;

	public MySQLInOperation(MySQLExpression expr, List<MySQLExpression> listElements, boolean isTrue) {
		this.expr = expr;
		this.listElements = listElements;
		this.isTrue = isTrue;
	}

	public MySQLExpression getExpr() {
		return expr;
	}

	public List<MySQLExpression> getListElements() {
		return listElements;
	}

	@Override
	public MySQLConstant getExpectedValue() {
		boolean allAreConstValues = listElements.stream().allMatch(e -> e instanceof MySQLConstant)
				&& expr instanceof MySQLConstant;
		MySQLConstant leftVal = expr.getExpectedValue();
		if (leftVal.isNull()) {
			return MySQLConstant.createNullConstant();
		}
		boolean isNull = false;
		for (MySQLExpression rightExpr : listElements) {
			MySQLConstant rightVal = rightExpr.getExpectedValue();
			MySQLConstant convertedRightVal;
			if (allAreConstValues) {
				// If all values are constants, they are evaluated according to the type of expr
				// and sorted.
				convertedRightVal = MySQLComputableFunction.castToMostGeneralType(rightVal, leftVal);
			} else {
				// Otherwise, type conversion takes place according to the rules described in
				// Section 12.2, “Type Conversion in Expression Evaluation”, but applied to all
				// the arguments.
				convertedRightVal = rightVal;
			}
			MySQLConstant isEquals = leftVal.isEquals(convertedRightVal);
			if (isEquals.isNull()) {
				isNull = true;
			} else {
				if (isEquals.getInt() == 1) {
					return MySQLConstant.createBoolean(isTrue);
				}
			}
		}
		if (isNull) {
			return MySQLConstant.createNullConstant();
		} else {
			return MySQLConstant.createBoolean(!isTrue);
		}

	}

	public boolean isTrue() {
		return isTrue;
	}
}
