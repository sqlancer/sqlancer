package lama.sqlite3.gen;

import java.util.ArrayList;
import java.util.List;

import lama.Randomly;
import lama.sqlite3.SQLite3Provider;
import lama.sqlite3.ast.SQLite3Case.CasePair;
import lama.sqlite3.ast.SQLite3Case.SQLite3CaseWithBaseExpression;
import lama.sqlite3.ast.SQLite3Case.SQLite3CaseWithoutBaseExpression;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation.BinaryOperator;
import lama.sqlite3.ast.SQLite3Expression.CollateOperation;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation.PostfixUnaryOperator;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Function;
import lama.sqlite3.ast.SQLite3Function.ComputableFunction;
import lama.sqlite3.ast.UnaryOperation;
import lama.sqlite3.ast.UnaryOperation.UnaryOperator;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;
import lama.sqlite3.schema.SQLite3Schema.RowValue;

public class SQLite3ExpressionGenerator {

	private enum LiteralValueType {
		INTEGER, NUMERIC, STRING, BLOB_LITERAL, NULL
	}

	private final RowValue rw;

	public SQLite3ExpressionGenerator() {
		this.rw = null;
	}

	public SQLite3ExpressionGenerator(RowValue rw) {
		this.rw = rw;
	}

	public static SQLite3Expression getRandomLiteralValue(boolean deterministicOnly, Randomly r) {
		return new SQLite3ExpressionGenerator().getRandomLiteralValueInt(deterministicOnly, r);
	}

	/***
	 * 
	 * @see https://www.sqlite.org/syntax/literal-value.html
	 * @return
	 */
	// TODO: real is hardcoded
	// TODO: blob is hardcoded
	public SQLite3Expression getRandomLiteralValueInt(boolean deterministicOnly, Randomly r) {
		while (true) {
			LiteralValueType randomLiteral = Randomly.fromOptions(LiteralValueType.values());
			switch (randomLiteral) {
			case INTEGER:
				return SQLite3Constant.createIntConstant(r.getInteger());
			case NUMERIC: // typeof(3.3) = real
				// see https://www.sqlite.org/syntax/numeric-literal.html
				return SQLite3Constant.createRealConstant(r.getDouble());
			case STRING:
				return SQLite3Constant.createTextConstant(r.getString());
			case BLOB_LITERAL: // typeof(X'ABCD') = blob
				return SQLite3Constant.getRandomBinaryConstant();
			case NULL: // typeof(NULL) = null
				return SQLite3Constant.createNullConstant();
			default:
				throw new AssertionError(randomLiteral);
			}
		}
	}

	enum ExpressionType {
		COLUMN_NAME, LITERAL_VALUE, UNARY_OPERATOR, POSTFIX_UNARY_OPERATOR, BINARY_OPERATOR, BETWEEN_OPERATOR,
		UNARY_FUNCTION, CAST_EXPRESSION, BINARY_COMPARISON_OPERATOR, KNOWN_RESULT_FUNCTION, IN_OPERATOR, COLLATE
		//, CASE_OPERATOR
	}

	public SQLite3Expression getRandomExpression(List<Column> columns, boolean deterministicOnly, Randomly r) {
		return getRandomExpression(columns, 0, deterministicOnly, r);
	}

	public SQLite3Expression getRandomExpression(List<Column> columns, int depth, boolean deterministicOnly,
			Randomly r) {
		if (depth >= SQLite3Provider.EXPRESSION_MAX_DEPTH) {
			if (Randomly.getBoolean()) {
				return getRandomLiteralValue(deterministicOnly, r);
			} else {
				Column c = Randomly.fromList(columns);
				return new ColumnName(c, rw == null ? null : rw.getValues().get(c));
			}
		}

		ExpressionType randomExpressionType = Randomly.fromOptions(ExpressionType.values());
		Column c;
		switch (randomExpressionType) {
		case UNARY_FUNCTION:
			String name = QueryGenerator.getRandomUnaryFunction();
			return new SQLite3Expression.Function(name, getRandomExpression(columns, depth + 1, deterministicOnly, r));
		case LITERAL_VALUE:
			return getRandomLiteralValue(deterministicOnly, r);
		case COLUMN_NAME:
			c = Randomly.fromList(columns);
			return new ColumnName(c, rw == null ? null : rw.getValues().get(c));
		case UNARY_OPERATOR:
			return getRandomUnaryOperator(columns, depth + 1, deterministicOnly, r);
		case POSTFIX_UNARY_OPERATOR:
			return getRandomPostfixUnaryOperator(columns, depth + 1, deterministicOnly, r);
		case BINARY_OPERATOR:
			return getBinaryOperator(columns, depth + 1, deterministicOnly, r);
		case BINARY_COMPARISON_OPERATOR:
			return getBinaryComparisonOperator(columns, depth + 1, deterministicOnly, r);
		case BETWEEN_OPERATOR:
			return getBetweenOperator(columns, depth + 1, deterministicOnly, r);
		case CAST_EXPRESSION:
			return getCastOperator(columns, depth + 1, deterministicOnly, r);
		case KNOWN_RESULT_FUNCTION:
			return getComputableFunction(columns, depth + 1, deterministicOnly, r);
		case IN_OPERATOR:
			return getInOperator(columns, depth + 1, deterministicOnly, r);
		case COLLATE:
			return new CollateOperation(getRandomExpression(columns, depth + 1, deterministicOnly, r),
					CollateSequence.random());
//		case CASE_OPERATOR:
//			return getCaseOperator(columns, depth + 1, deterministicOnly, r);
		default:
			throw new AssertionError(randomExpressionType);
		}
	}

	private SQLite3Expression getCaseOperator(List<Column> columns, int depth, boolean deterministicOnly, Randomly r) {
		int nrCaseExpressions = 1 + Randomly.smallNumber();
		CasePair[] pairs = new CasePair[nrCaseExpressions];
		for (int i = 0; i < pairs.length; i++) {
			SQLite3Expression whenExpr = getRandomExpression(columns, depth + 1, deterministicOnly, r);
			SQLite3Expression thenExpr = getRandomExpression(columns, depth + 1, deterministicOnly, r);
			CasePair pair = new CasePair(whenExpr, thenExpr);
			pairs[i] = pair;
		}
		SQLite3Expression elseExpr;
		if (Randomly.getBoolean()) {
			elseExpr = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		} else {
			elseExpr = null;
		}
		if (Randomly.getBoolean()) {
			return new SQLite3CaseWithoutBaseExpression(pairs, elseExpr);
		} else {
			SQLite3Expression baseExpr = getRandomExpression(columns, depth + 1, deterministicOnly, r);
			return new SQLite3CaseWithBaseExpression(baseExpr, pairs, elseExpr);
		}
	}

	private SQLite3Expression getCastOperator(List<Column> columns, int depth, boolean deterministicOnly, Randomly r) {
		SQLite3Expression expr = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		TypeLiteral type = new SQLite3Expression.TypeLiteral(
				Randomly.fromOptions(SQLite3Expression.TypeLiteral.Type.values()));
		return new SQLite3Expression.Cast(type, expr);
	}

	private SQLite3Expression getComputableFunction(List<Column> columns, int depth, boolean deterministicOnly,
			Randomly r) {
		ComputableFunction func = ComputableFunction.getRandomFunction();
		int nrArgs = func.getNrArgs();
		if (func.isVariadic()) {
			nrArgs += Randomly.smallNumber();
		}
		SQLite3Expression[] args = new SQLite3Expression[nrArgs];
		for (int i = 0; i < args.length; i++) {
			args[i] = getRandomExpression(columns, depth + 1, deterministicOnly, r);
			if (i == 0 && Randomly.getBoolean()) {
				args[i] = new SQLite3Distinct(args[i]);
			}
		}
		SQLite3Function sqlFunction = new SQLite3Function(func, args);
		return sqlFunction;
	}

	private SQLite3Expression getBetweenOperator(List<Column> columns, int depth, boolean deterministicOnly,
			Randomly r) {
		boolean tr = Randomly.getBoolean();
		SQLite3Expression expr = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		SQLite3Expression left = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		SQLite3Expression right = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		return new SQLite3Expression.BetweenOperation(expr, tr, left, right);
	}

	// TODO: incomplete
	private SQLite3Expression getBinaryOperator(List<Column> columns, int depth, boolean deterministicOnly,
			Randomly r) {
		SQLite3Expression leftExpression = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		// TODO: operators
		BinaryOperator operator = BinaryOperator.getRandomOperator();
		while (operator == BinaryOperator.DIVIDE) {
			operator = BinaryOperator.getRandomOperator();
		}
		SQLite3Expression rightExpression = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		return new SQLite3Expression.BinaryOperation(leftExpression, rightExpression, operator);
	}

	private SQLite3Expression getInOperator(List<Column> columns, int depth, boolean deterministicOnly, Randomly r) {
		SQLite3Expression leftExpression = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		List<SQLite3Expression> right = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber(); i++) {
			right.add(getRandomExpression(columns, depth + 1, deterministicOnly, r));
		}
		return new SQLite3Expression.InOperation(leftExpression, right);
	}

	private SQLite3Expression getBinaryComparisonOperator(List<Column> columns, int depth, boolean deterministicOnly,
			Randomly r) {
		SQLite3Expression leftExpression = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		BinaryComparisonOperator operator = BinaryComparisonOperator.getRandomOperator();
		SQLite3Expression rightExpression = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		return new SQLite3Expression.BinaryComparisonOperation(leftExpression, rightExpression, operator);
	}

	// complete
	private SQLite3Expression getRandomPostfixUnaryOperator(List<Column> columns, int depth, boolean deterministicOnly,
			Randomly r) {
		SQLite3Expression subExpression = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		PostfixUnaryOperator operator = PostfixUnaryOperator.getRandomOperator();
		return new SQLite3Expression.PostfixUnaryOperation(operator, subExpression);
	}

	// complete
	public SQLite3Expression getRandomUnaryOperator(List<Column> columns, int depth, boolean deterministicOnly,
			Randomly r) {
		SQLite3Expression subExpression = getRandomExpression(columns, depth + 1, deterministicOnly, r);
		UnaryOperator unaryOperation = Randomly.fromOptions(UnaryOperator.values());
		return new UnaryOperation(unaryOperation, subExpression);
	}

}
