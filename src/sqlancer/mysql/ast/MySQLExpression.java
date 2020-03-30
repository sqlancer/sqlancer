package sqlancer.mysql.ast;

public abstract class MySQLExpression {
	
	public MySQLConstant getExpectedValue() {
		throw new AssertionError("PQS not supported for this operator");
	}

}
