package lama.tablegen.sqlite3;

import java.util.List;

import lama.Expression;
import lama.Expression.BinaryOperation.BinaryOperator;
import lama.Expression.ColumnName;
import lama.Expression.Constant;
import lama.Expression.PostfixUnaryOperation.PostfixUnaryOperator;
import lama.Expression.UnaryOperation.UnaryOperator;
import lama.Main;
import lama.QueryGenerator;
import lama.Randomly;
import lama.schema.Schema.Column;

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
	public static Expression getRandomLiteralValue(boolean deterministicOnly) {
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
		COLUMN_NAME, LITERAL_VALUE, UNARY_OPERATOR, POSTFIX_UNARY_OPERATOR, BINARY_OPERATOR, BETWEEN_OPERATOR, UNARY_FUNCTION
	}

	public static Expression getRandomExpression(List<Column> columns, boolean deterministicOnly) {
		return getRandomExpression(columns, 0, deterministicOnly);
	}

	public static Expression getRandomExpression(List<Column> columns, int depth, boolean deterministicOnly) {
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
			return new Expression.Function(name, getRandomExpression(columns, deterministicOnly));
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
		default:
			throw new AssertionError(randomExpressionType);
		}
	}

	private static Expression getBetweenOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		boolean tr = Randomly.getBoolean();
		Expression expr = getRandomExpression(columns, depth + 1, deterministicOnly);
		Expression left = getRandomExpression(columns, depth + 1, deterministicOnly);
		Expression right = getRandomExpression(columns, depth + 1, deterministicOnly);
		return new Expression.BetweenOperation(expr, tr, left, right);
	}

	// TODO: incomplete
	private static Expression getBinaryOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		Expression leftExpression = getRandomExpression(columns, depth + 1, deterministicOnly);
		// TODO: operators
		BinaryOperator operator = BinaryOperator.getRandomOperator();
		Expression rightExpression = getRandomExpression(columns, depth + 1, deterministicOnly);
		return new Expression.BinaryOperation(leftExpression, rightExpression, operator);
	}

	// complete
	private static Expression getRandomPostfixUnaryOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		Expression subExpression = getRandomExpression(columns, depth + 1, deterministicOnly);
		PostfixUnaryOperator operator = PostfixUnaryOperator.getRandomOperator();
		return new Expression.PostfixUnaryOperation(operator, subExpression);
	}

	// complete
	public static Expression getRandomUnaryOperator(List<Column> columns, int depth, boolean deterministicOnly) {
		Expression subExpression = getRandomExpression(columns, depth + 1, deterministicOnly);
		UnaryOperator unaryOperation = Randomly.fromOptions(UnaryOperator.values());
		return new Expression.UnaryOperation(unaryOperation, subExpression);
	}

}
