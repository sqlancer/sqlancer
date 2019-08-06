package lama.sqlite3;

import lama.sqlite3.ast.SQLite3Aggregate;
import lama.sqlite3.ast.SQLite3Case.SQLite3CaseWithBaseExpression;
import lama.sqlite3.ast.SQLite3Case.SQLite3CaseWithoutBaseExpression;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BetweenOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation;
import lama.sqlite3.ast.SQLite3Expression.Cast;
import lama.sqlite3.ast.SQLite3Expression.CollateOperation;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.Exist;
import lama.sqlite3.ast.SQLite3Expression.Function;
import lama.sqlite3.ast.SQLite3Expression.InOperation;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.OrderingTerm;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import lama.sqlite3.ast.SQLite3Expression.Subquery;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Function;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.UnaryOperation;

public abstract class SQLite3Visitor {

	public boolean fullyQualifiedNames = true;

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

	public abstract void visit(BinaryComparisonOperation op);

	public abstract void visit(BinaryOperation op);

	public abstract void visit(BetweenOperation op);

	public abstract void visit(ColumnName c);

	public abstract void visit(SQLite3Constant c);

	public abstract void visit(Function f);

	public abstract void visit(SQLite3SelectStatement s);

	public abstract void visit(OrderingTerm term);

	public abstract void visit(UnaryOperation exp);

	public abstract void visit(PostfixUnaryOperation exp);

	public abstract void visit(CollateOperation op);

	public abstract void visit(Cast cast);

	public abstract void visit(TypeLiteral literal);

	public abstract void visit(InOperation op);

	public abstract void visit(Subquery query);

	public abstract void visit(Exist exist);

	public abstract void visit(Join join);

	public abstract void visit(SQLite3Function func);

	public abstract void visit(SQLite3Distinct distinct);

	public abstract void visit(SQLite3CaseWithoutBaseExpression casExpr);
	
	public abstract void visit(SQLite3CaseWithBaseExpression casExpr);

	public abstract void visit(SQLite3Aggregate aggr);

	public void visit(SQLite3Expression expr) {
		if (expr instanceof BinaryOperation) {
			visit((BinaryOperation) expr);
		} else if (expr instanceof ColumnName) {
			visit((ColumnName) expr);
		} else if (expr instanceof SQLite3Constant) {
			visit((SQLite3Constant) expr);
		} else if (expr instanceof UnaryOperation) {
			visit((UnaryOperation) expr);
		} else if (expr instanceof PostfixUnaryOperation) {
			visit((PostfixUnaryOperation) expr);
		} else if (expr instanceof Function) {
			visit((Function) expr);
		} else if (expr instanceof BetweenOperation) {
			visit((BetweenOperation) expr);
		} else if (expr instanceof CollateOperation) {
			visit((CollateOperation) expr);
		} else if (expr instanceof OrderingTerm) {
			visit((OrderingTerm) expr);
		} else if (expr instanceof SQLite3Expression.InOperation) {
			visit((InOperation) expr);
		} else if (expr instanceof Cast) {
			visit((Cast) expr);
		} else if (expr instanceof Subquery) {
			visit((Subquery) expr);
		} else if (expr instanceof Join) {
			visit((Join) expr);
		} else if (expr instanceof SQLite3SelectStatement) {
			visit((SQLite3SelectStatement) expr);
		} else if (expr instanceof Exist) {
			visit((Exist) expr);
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
		} else {
			throw new AssertionError(expr);
		}
	}

	public static String asString(SQLite3Expression expr) {
		if (expr == null) {
			throw new AssertionError();
		}
		SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

	public static String asExpectedValues(SQLite3Expression expr) {
		SQLite3ExpectedValueVisitor visitor = new SQLite3ExpectedValueVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

}
