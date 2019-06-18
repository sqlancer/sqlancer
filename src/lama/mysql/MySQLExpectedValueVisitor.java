package lama.mysql;

import lama.mysql.ast.MySQLBinaryLogicalOperation;
import lama.mysql.ast.MySQLColumnValue;
import lama.mysql.ast.MySQLComputableFunction;
import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLExpression;
import lama.mysql.ast.MySQLJoin;
import lama.mysql.ast.MySQLSelect;
import lama.mysql.ast.MySQLUnaryNotOperator;
import lama.mysql.ast.MySQLUnaryPostfixOperator;

public class MySQLExpectedValueVisitor extends MySQLVisitor {

	private final StringBuilder sb = new StringBuilder();
	private int nrTabs = 0;

	private void print(MySQLExpression expr) {
		MySQLToStringVisitor v = new MySQLToStringVisitor();
		v.visit(expr);
		for (int i = 0; i < nrTabs; i++) {
			sb.append("\t");
		}
		sb.append(v.get());
		sb.append(" -- " + expr.getExpectedValue());
		sb.append("\n");
	}

	@Override
	public void visit(MySQLExpression expr) {
		nrTabs++;
		super.visit(expr);
		nrTabs--;
	}

	@Override
	public void visit(MySQLConstant constant) {
		print(constant);
	}

	@Override
	public void visit(MySQLColumnValue column) {
		print(column);
	}

	@Override
	public void visit(MySQLUnaryNotOperator op) {
		print(op);
		visit(op.getExpression());
	}

	@Override
	public void visit(MySQLUnaryPostfixOperator op) {
		print(op);
		visit(op.getExpression());
	}

	@Override
	public void visit(MySQLComputableFunction f) {
		print(f);
		for (MySQLExpression expr : f.getArguments()) {
			visit(expr);
		}
	}

	@Override
	public void visit(MySQLBinaryLogicalOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	public String get() {
		return sb.toString();
	}

	@Override
	public void visit(MySQLSelect select) {
		for (MySQLJoin j : select.getJoinClauses()) {
			visit(j);
		}
		visit(select.getWhereClause());
	}

}
