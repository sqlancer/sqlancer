package lama.mysql.ast;

import lama.mysql.MySQLSchema.MySQLColumn;

public class MySQLColumnValue extends MySQLExpression {
	
	private final MySQLColumn column;
	private final MySQLConstant value;

	private MySQLColumnValue(MySQLColumn column, MySQLConstant value) {
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
