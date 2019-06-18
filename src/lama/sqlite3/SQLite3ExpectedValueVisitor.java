package lama.sqlite3;

import lama.sqlite3.ast.SQLite3Case.CasePair;
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
import lama.sqlite3.ast.SQLite3Expression.LogicalOperation;
import lama.sqlite3.ast.SQLite3Expression.OrderingTerm;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import lama.sqlite3.ast.SQLite3Expression.Subquery;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Function;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.UnaryOperation;

public class SQLite3ExpectedValueVisitor extends SQLite3Visitor {

	private final StringBuilder sb = new StringBuilder();
	private int nrTabs = 0;

	private void print(SQLite3Expression expr) {
		SQLite3ToStringVisitor v = new SQLite3ToStringVisitor();
		v.visit(expr);
		for (int i = 0; i < nrTabs; i++) {
			sb.append("\t");
		}
		sb.append(v.get());
		sb.append(" -- " + expr.getExpectedValue());
		sb.append("\n");
	}

	@Override
	public void visit(SQLite3Expression expr) {
		nrTabs++;
		super.visit(expr);
		nrTabs--;
	}

	@Override
	public void visit(BinaryOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(LogicalOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(BetweenOperation op) {
		print(op);
		visit(op.getExpression());
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(ColumnName c) {
		print(c);
	}

	@Override
	public void visit(SQLite3Constant c) {
		print(c);
	}

	@Override
	public void visit(Function f) {
		print(f);
		for (SQLite3Expression expr : f.getArguments()) {
			visit(expr);
		}
	}

	@Override
	public void visit(SQLite3SelectStatement s) {
		visit(s.getWhereClause());
	}

	@Override
	public void visit(OrderingTerm term) {
		print(term);
		visit(term.getExpression());
	}

	@Override
	public void visit(UnaryOperation exp) {
		print(exp);
		visit(exp.getExpression());
	}

	@Override
	public void visit(PostfixUnaryOperation exp) {
		print(exp);
		visit(exp.getExpression());
	}

	@Override
	public void visit(CollateOperation op) {
		print(op);
		visit(op.getExpression());
	}

	@Override
	public void visit(Cast cast) {
		print(cast);
		visit(cast.getExpression());
	}

	@Override
	public void visit(TypeLiteral literal) {
	}

	@Override
	public void visit(InOperation op) {
		print(op);
		visit(op.getLeft());
		for (SQLite3Expression expr : op.getRight()) {
			visit(expr);
		}
	}

	@Override
	public void visit(Subquery query) {
		print(query);
		visit(query.getExpectedValue());
	}

	@Override
	public void visit(Exist exist) {
		print(exist);
		visit(exist.getSelect());
	}

	@Override
	public void visit(Join join) {
		print(join);
		visit(join.getOnClause());
	}

	@Override
	public void visit(BinaryComparisonOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	public String get() {
		return sb.toString();
	}

	@Override
	public void visit(SQLite3Function func) {
		print(func);
		for (SQLite3Expression expr : func.getArgs()) {
			visit(expr);
		}
	}

	@Override
	public void visit(SQLite3Distinct distinct) {
		print(distinct);
		visit(distinct.getExpression());
	}

	@Override
	public void visit(SQLite3CaseWithoutBaseExpression caseExpr) {
		for (CasePair cExpr : caseExpr.getPairs()) {
			print(cExpr.getCond());
			visit(cExpr.getCond());
			print(cExpr.getThen());
			visit(cExpr.getThen());
		}
		if (caseExpr.getElseExpr() != null) {
			print(caseExpr.getElseExpr());
			visit(caseExpr.getElseExpr());
		}
	}

	@Override
	public void visit(SQLite3CaseWithBaseExpression caseExpr) {
		print(caseExpr);
		visit(caseExpr.getBaseExpr());
		for (CasePair cExpr : caseExpr.getPairs()) {
			print(cExpr.getCond());
			visit(cExpr.getCond());
			print(cExpr.getThen());
			visit(cExpr.getThen());
		}
		if (caseExpr.getElseExpr() != null) {
			print(caseExpr.getElseExpr());
			visit(caseExpr.getElseExpr());
		}
	}

}
