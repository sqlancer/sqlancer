package lama.sqlite3.gen;

import java.util.List;

import lama.Main;
import lama.Randomly;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.Constant;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation.BinaryOperator;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation.PostfixUnaryOperator;
import lama.sqlite3.ast.SQLite3Expression.UnaryOperation.UnaryOperator;
import lama.sqlite3.schema.SQLite3Schema.Column;

public class SQLite3ExpressionGenerator {

	private enum LiteralValueType {
		INTEGER, NUMERIC, STRING, BLOB_LITERAL, NULL, TRUE, FALSE
	}

	/***
	 * 
	 * @see https://www.sqlite.org/syntax/literal-value.html
	 * @return
	 */
	// TODO: real is hardcoded
	// TODO: blob is hardcoded
	public static SQLite3Expression getRandomLiteralValue(boolean deterministicOnly) {
		while (true) {
			LiteralValueType randomLiteral = Randomly.fromOptions(LiteralValueType.values());
			switch (randomLiteral) {
			case INTEGER:
				return Constant.createIntConstant(Randomly.getInteger());
			case NUMERIC: // typeof(3.3) = real
				// see https://www.sqlite.org/syntax/numeric-literal.html
				return Constant.createRealConstant(Randomly.getDouble());
			case STRING:
				return Constant.createTextConstant(Randomly.getString());
			case BLOB_LITERAL: // typeof(X'ABCD') = blob
				return Constant.getRandomBinaryConstant();
			case NULL: // typeof(NULL) = null
				return Constant.createNullConstant();
			case TRUE: // typeof(TRUE) = integer
				return Constant.createBooleanConstant(true);
			case FALSE: // typeof(FALSE) = integer
				return Constant.createBooleanConstant(false);
			default:
				throw new AssertionError(randomLiteral);
			}
		}
	}

	enum ExpressionType {
		COLUMN_NAME, LITERAL_VALUE, UNARY_OPERATOR, POSTFIX_UNARY_OPERATOR, BINARY_OPERATOR, BETWEEN_OPERATOR, UNARY_FUNCTION, CAST_EXPRESSION
	}

	public static SQLite3Expression getRandomExpression(List<Column> columns, boolean deterministicOnly) {
		return getRandomExpression(columns, 0, deterministicOnly);
	}

	public static SQLite3Expression getRandomExpression(List<Column> columns, int depth, boolean deterministicOnly) {
		if (depth >= Main.EXPRESSION_MAX_DEPTH) {
			if (Randomly.getBoolean()) {
				return getRandomLiteralValue(deterministicOnly);
			} else {
				return new ColumnName(Randomly.fromList(columns));
			}
		}

		ExpressionType randomExpressionType = Randomly.fromOptions(ExpressionType.values());
		switch (randomExpressionType) {
		case UNARY_FUNCTION:
			String name = QueryGenerator.getRandomUnaryFunction();
			return new SQLite3Expression.Function(name, getRandomExpression(columns, deterministicOnly));
		case LITERAL_VALUE:
			return getRandomLiteralValue(deterministicOnly);
		case COLUMN_NAME:
			return new ColumnName(Randomly.fromList(columns));
		case UNARY_OPERATOR:
			return SQLite3ExpressionGenerator.getRandomUnaryOperator(columns, depth + 1, deterministicOnly);
		case POSTFIX_UNARY_OPERATOR:
			return SQLite3ExpressionGenerator.getRandomPostfixUnaryOperator(columns, depth + 1, deterministicOnly);
		case BINARY_OPERATOR:
			return SQLite3ExpressionGenerator.getBinaryOperator(columns, depth + 1, deterministicOnly);
		case BETWEEN_OPERATOR:
			return SQLite3ExpressionGenerator.getBetweenOperator(columns, depth + 1, deterministicOnly);
		case CAST_EXPRESSION:
			return getCastOperator(columns, depth + 1, deterministicOnly);
		default:
			throw new AssertionError(randomExpressionType);
		}
	}

	private static SQLite3Expression getCastOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		SQLite3Expression expr = getRandomExpression(columns, depth + 1, deterministicOnly);
		SQLite3Expression type = new SQLite3Expression.TypeLiteral(Randomly.fromOptions(SQLite3Expression.TypeLiteral.Type.values()));
		return new SQLite3Expression.Cast(type, expr);
	}

	private static SQLite3Expression getBetweenOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		boolean tr = Randomly.getBoolean();
		SQLite3Expression expr = getRandomExpression(columns, depth + 1, deterministicOnly);
		SQLite3Expression left = getRandomExpression(columns, depth + 1, deterministicOnly);
		SQLite3Expression right = getRandomExpression(columns, depth + 1, deterministicOnly);
		return new SQLite3Expression.BetweenOperation(expr, tr, left, right);
	}

	// TODO: incomplete
	private static SQLite3Expression getBinaryOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		SQLite3Expression leftExpression = getRandomExpression(columns, depth + 1, deterministicOnly);
		// TODO: operators
		BinaryOperator operator = BinaryOperator.getRandomOperator();
		SQLite3Expression rightExpression = getRandomExpression(columns, depth + 1, deterministicOnly);
		return new SQLite3Expression.BinaryOperation(leftExpression, rightExpression, operator);
	}

	// complete
	private static SQLite3Expression getRandomPostfixUnaryOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		SQLite3Expression subExpression = getRandomExpression(columns, depth + 1, deterministicOnly);
		PostfixUnaryOperator operator = PostfixUnaryOperator.getRandomOperator();
		return new SQLite3Expression.PostfixUnaryOperation(operator, subExpression);
	}

	// complete
	public static SQLite3Expression getRandomUnaryOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		SQLite3Expression subExpression = getRandomExpression(columns, depth + 1, deterministicOnly);
		UnaryOperator unaryOperation = Randomly.fromOptions(UnaryOperator.values());
		return new SQLite3Expression.UnaryOperation(unaryOperation, subExpression);
	}

}
