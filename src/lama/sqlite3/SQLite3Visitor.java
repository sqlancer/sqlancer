package lama.sqlite3;

import lama.sqlite3.ast.SQLite3Aggregate;
import lama.sqlite3.ast.SQLite3Case.SQLite3CaseWithBaseExpression;
import lama.sqlite3.ast.SQLite3Case.SQLite3CaseWithoutBaseExpression;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BetweenOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import lama.sqlite3.ast.SQLite3Expression.Cast;
import lama.sqlite3.ast.SQLite3Expression.CollateOperation;
import lama.sqlite3.ast.SQLite3Expression.Function;
import lama.sqlite3.ast.SQLite3Expression.InOperation;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.MatchOperation;
import lama.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Exist;
import lama.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.SQLite3TableReference;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Text;
import lama.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation;
import lama.sqlite3.ast.SQLite3Expression.Subquery;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Function;
import lama.sqlite3.ast.SQLite3RowValue;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3SetClause;
import lama.sqlite3.ast.SQLite3UnaryOperation;
import lama.sqlite3.ast.SQLite3WindowFunction;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecBetween;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecTerm;

public interface SQLite3Visitor {


	public static byte[] hexStringToByteArray(String s) {
		byte[] b = new byte[s.length() / 2];
		for (int i = 0; i < b.length; i++) {
			int index = i * 2;
			int v = Integer.parseInt(s.substring(index, index + 2), 16);
			b[i] = (byte) v;
		}
		return b;
	}

	public static String byteArrayToHex(byte[] a) {
		StringBuilder sb = new StringBuilder(a.length * 2);
		for (byte b : a)
			sb.append(String.format("%02x", b));
		return sb.toString();
	}

	
	
	// TODO remove these default methods
	
	public default void visit(BinaryComparisonOperation op) {
		
	}

	public default void visit(Sqlite3BinaryOperation op) {
		
	}
	
	public default void visit(SQLite3UnaryOperation exp) {
		
	}

	public default void visit(SQLite3PostfixText op) {
		
	}
	
	public default void visit(SQLite3PostfixUnaryOperation exp) {
		
	}

	public abstract void visit(BetweenOperation op);

	public abstract void visit(SQLite3ColumnName c);

	public abstract void visit(SQLite3Constant c);

	public abstract void visit(Function f);

	public abstract void visit(SQLite3SelectStatement s, boolean inner);

	public abstract void visit(SQLite3OrderingTerm term);
	
	public abstract void visit (SQLite3TableReference tableReference);

	public abstract void visit(SQLite3SetClause set);
	
	public abstract void visit(CollateOperation op);

	public abstract void visit(Cast cast);

	public abstract void visit(TypeLiteral literal);

	public abstract void visit(InOperation op);

	public abstract void visit(Subquery query);

	public abstract void visit(SQLite3Exist exist);

	public abstract void visit(Join join);
	
	public abstract void visit(MatchOperation match);

	public abstract void visit(SQLite3Function func);
	
	public abstract void visit(SQLite3Text func);

	public abstract void visit(SQLite3Distinct distinct);

	public abstract void visit(SQLite3CaseWithoutBaseExpression casExpr);
	
	public abstract void visit(SQLite3CaseWithBaseExpression casExpr);

	public abstract void visit(SQLite3Aggregate aggr);
	
	public abstract void visit(SQLite3WindowFunction func);
	
	public abstract void visit(SQLite3RowValue rw);
	
	public abstract void visit(SQLite3WindowFunctionExpression windowFunction);
	
	public abstract void visit(SQLite3WindowFunctionFrameSpecTerm term);
	
	public abstract void visit(SQLite3WindowFunctionFrameSpecBetween between);
	
	public default void visit(SQLite3Expression expr) {
		if (expr instanceof Sqlite3BinaryOperation) {
			visit((Sqlite3BinaryOperation) expr);
		} else if (expr instanceof SQLite3ColumnName) {
			visit((SQLite3ColumnName) expr);
		} else if (expr instanceof SQLite3Constant) {
			visit((SQLite3Constant) expr);
		} else if (expr instanceof SQLite3UnaryOperation) {
			visit((SQLite3UnaryOperation) expr);
		} else if (expr instanceof SQLite3PostfixUnaryOperation) {
			visit((SQLite3PostfixUnaryOperation) expr);
		} else if (expr instanceof Function) {
			visit((Function) expr);
		} else if (expr instanceof BetweenOperation) {
			visit((BetweenOperation) expr);
		} else if (expr instanceof CollateOperation) {
			visit((CollateOperation) expr);
		} else if (expr instanceof SQLite3OrderingTerm) {
			visit((SQLite3OrderingTerm) expr);
		} else if (expr instanceof SQLite3Expression.InOperation) {
			visit((InOperation) expr);
		} else if (expr instanceof Cast) {
			visit((Cast) expr);
		} else if (expr instanceof Subquery) {
			visit((Subquery) expr);
		} else if (expr instanceof Join) {
			visit((Join) expr);
		} else if (expr instanceof SQLite3SelectStatement) {
			visit((SQLite3SelectStatement) expr, true);
		} else if (expr instanceof SQLite3Exist) {
			visit((SQLite3Exist) expr);
		} else if (expr instanceof BinaryComparisonOperation) {
			visit((BinaryComparisonOperation) expr);
		} else if (expr instanceof SQLite3Function) {
			visit((SQLite3Function) expr);
		} else if (expr instanceof SQLite3Distinct) {
			visit((SQLite3Distinct) expr);
		} else if (expr instanceof SQLite3CaseWithoutBaseExpression) {
			visit((SQLite3CaseWithoutBaseExpression) expr);
		} else if (expr instanceof SQLite3CaseWithBaseExpression) {
			visit((SQLite3CaseWithBaseExpression) expr);
		} else if (expr instanceof SQLite3Aggregate) {
			visit((SQLite3Aggregate) expr);
		} else if (expr instanceof SQLite3PostfixText) {
			visit((SQLite3PostfixText) expr);
		} else if (expr instanceof SQLite3WindowFunction) {
			visit((SQLite3WindowFunction) expr);
		} else if (expr instanceof MatchOperation) {
			visit((MatchOperation) expr);
		} else if (expr instanceof SQLite3RowValue) {
			visit((SQLite3RowValue) expr);
		} else if (expr instanceof SQLite3Text) {
			visit((SQLite3Text) expr);
		} else if (expr instanceof SQLite3WindowFunctionExpression) {
			visit((SQLite3WindowFunctionExpression) expr);
		} else if (expr instanceof SQLite3WindowFunctionFrameSpecTerm) {
			visit((SQLite3WindowFunctionFrameSpecTerm) expr);
		} else if (expr instanceof SQLite3WindowFunctionFrameSpecBetween) {
			visit((SQLite3WindowFunctionFrameSpecBetween) expr);
		} else if (expr instanceof SQLite3TableReference) {
			visit((SQLite3TableReference) expr);
		} else if (expr instanceof SQLite3SetClause) {
			visit((SQLite3SetClause) expr);
		} else {
			throw new AssertionError(expr);
		}
	}

	public static String asString(SQLite3Expression expr) {
		if (expr == null) {
			throw new AssertionError();
		}
		SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
		if (expr instanceof SQLite3SelectStatement) {
			visitor.visit((SQLite3SelectStatement) expr, false);
		} else {
			visitor.visit(expr);
		}
		return visitor.get();
	}
	

	public static String asExpectedValues(SQLite3Expression expr) {
		SQLite3ExpectedValueVisitor visitor = new SQLite3ExpectedValueVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

}
