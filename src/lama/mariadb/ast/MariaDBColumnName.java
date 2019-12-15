package lama.mariadb.ast;

import lama.mariadb.MariaDBSchema.MariaDBColumn;

public class MariaDBColumnName extends MariaDBExpression {
	
	private MariaDBColumn column;

	public MariaDBColumnName(MariaDBColumn column) {
		this.column = column;
	}
	
	public MariaDBColumn getColumn() {
		return column;
	}

}
