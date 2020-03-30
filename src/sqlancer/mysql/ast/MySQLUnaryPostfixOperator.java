package sqlancer.mysql.ast;

import sqlancer.mysql.ast.MySQLConstant.MySQLIntConstant;

public class MySQLUnaryPostfixOperator implements MySQLExpression {

	private final MySQLExpression expression;
	private final UnaryPostfixOperator operator;
	private boolean negate;

	public enum UnaryPostfixOperator {
		IS_NULL, IS_TRUE, IS_FALSE;
	}

	public MySQLUnaryPostfixOperator(MySQLExpression expr, UnaryPostfixOperator op, boolean negate) {
		this.expression = expr;
		this.operator = op;
		this.setNegate(negate);
	}

	public MySQLExpression getExpression() {
		return expression;
	}

	public UnaryPostfixOperator getOperator() {
		return operator;
	}

	public boolean isNegated() {
		return negate;
	}

	public void setNegate(boolean negate) {
		this.negate = negate;
	}

	@Override
	public MySQLConstant getExpectedValue() {
		boolean val;
		MySQLConstant expectedValue = expression.getExpectedValue();
		switch (operator) {
		case IS_NULL:
			val = expectedValue.isNull();
			break;
		case IS_FALSE:
			val = !expectedValue.isNull() && !expectedValue.asBooleanNotNull();
			break;
		case IS_TRUE:
			val = !expectedValue.isNull() && expectedValue.asBooleanNotNull();
			break;
		default:
			throw new AssertionError(operator);
		}
		if (negate) {
			val = !val;
		}
		return MySQLIntConstant.createIntConstant(val ? 1 : 0);
	}

}
