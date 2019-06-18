package lama.sqlite3;

import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.Cast;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.UnaryOperation;
import lama.sqlite3.ast.UnaryOperation.UnaryOperator;

public class SQLite3CollateHelper {

	public static boolean shouldGetSubexpressionAffinity(SQLite3Expression expression) {
		return (expression instanceof UnaryOperation && ((UnaryOperation) expression).getOperation() == UnaryOperator.PLUS) || expression instanceof Cast || expression instanceof ColumnName;
	}
	
}
