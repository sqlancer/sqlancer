package lama.mysql.ast;

public class MySQLUnaryNotOperator extends MySQLExpression {

	private final MySQLExpression expr;
	
	public MySQLUnaryNotOperator(MySQLExpression expr) {
		this.expr = expr;
	}

	public MySQLExpression getExpression() {
		return expr;
	}

	@Override
	public MySQLConstant getExpectedValue() {
		MySQLConstant subExprVal = expr.getExpectedValue();
		if (subExprVal.isNull()) {
			return MySQLConstant.createNullConstant();
		} else {
			return MySQLConstant.createIntConstant(subExprVal.asBooleanNotNull() ? 0 : 1);
		}
	}
	
}
