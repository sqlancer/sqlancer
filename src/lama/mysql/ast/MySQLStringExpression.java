package lama.mysql.ast;

public class MySQLStringExpression extends MySQLExpression {

	private final String str;
	private final MySQLConstant expectedValue;

	public MySQLStringExpression(String str, MySQLConstant expectedValue) {
		this.str = str;
		this.expectedValue = expectedValue;
	}

	public String getStr() {
		return str;
	}

	@Override
	public MySQLConstant getExpectedValue() {
		return expectedValue;
	}

}
