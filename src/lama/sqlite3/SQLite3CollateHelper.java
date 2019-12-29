package lama.sqlite3;

import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.Cast;
import lama.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import lama.sqlite3.ast.SQLite3UnaryOperation;
import lama.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;

public class SQLite3CollateHelper {

	public static boolean shouldGetSubexpressionAffinity(SQLite3Expression expression) {
		return (expression instanceof SQLite3UnaryOperation && ((SQLite3UnaryOperation) expression).getOperation() == UnaryOperator.PLUS) || expression instanceof Cast || expression instanceof SQLite3ColumnName;
	}
	
}
