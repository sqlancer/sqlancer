package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLColumn;

public class MySQLColumnValue implements MySQLExpression {
	
	private final MySQLColumn column;
	private final MySQLConstant value;

	public MySQLColumnValue(MySQLColumn column, MySQLConstant value) {
		this.column = column;
		this.value = value;
	}

	public static MySQLColumnValue create(MySQLColumn column, MySQLConstant value) {
		return new MySQLColumnValue(column, value);
	}

	public MySQLColumn getColumn() {
		return column;
	}

	public MySQLConstant getValue() {
		return value;
	}

	@Override
	public MySQLConstant getExpectedValue() {
		return value;
	}

}
