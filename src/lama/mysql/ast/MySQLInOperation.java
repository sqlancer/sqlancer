package lama.mysql.ast;

import java.util.List;

import lama.IgnoreMeException;

public class MySQLInOperation extends MySQLExpression {

	private final MySQLExpression expr;
	private final List<MySQLExpression> listElements;
	
	public MySQLInOperation(MySQLExpression expr, List<MySQLExpression> listElements) {
		this.expr = expr;
		this.listElements = listElements;
	}
	
	
	public MySQLExpression getExpr() {
		return expr;
	}
	
	public List<MySQLExpression> getListElements() {
		return listElements;
	}


	@Override
	public MySQLConstant getExpectedValue() {
		boolean allAreConstValues = listElements.stream().allMatch(e -> e instanceof MySQLConstant) && expr instanceof MySQLConstant;
		MySQLConstant leftVal = expr.getExpectedValue();
		if (leftVal.isNull()) {
			return MySQLConstant.createNullConstant();
		}
		boolean isNull = false;
		if (allAreConstValues) {
			for (MySQLExpression rightExpr : listElements) {
				MySQLConstant rightVal = rightExpr.getExpectedValue();
				MySQLConstant convertedRightVal = MySQLComputableFunction.castToMostGeneralType(rightVal, leftVal);
				MySQLConstant isEquals = leftVal.isEquals(convertedRightVal);
				if (isEquals.isNull()) {
					isNull = true;
				} else {
					if (isEquals.getInt() == 1) {
						return MySQLConstant.createTrue();
					}
				}
			}
			if (isNull) {
				return MySQLConstant.createNullConstant();
			} else {
				return MySQLConstant.createFalse();
			}
		} else {
			throw new IgnoreMeException();
		}
		
	}
}
