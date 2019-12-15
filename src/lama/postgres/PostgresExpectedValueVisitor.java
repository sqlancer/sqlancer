package lama.postgres;

import lama.IgnoreMeException;
import lama.postgres.ast.PostgresAggregate;
import lama.postgres.ast.PostgresBetweenOperation;
import lama.postgres.ast.PostgresBinaryOperation;
import lama.postgres.ast.PostgresCastOperation;
import lama.postgres.ast.PostgresCollate;
import lama.postgres.ast.PostgresColumnValue;
import lama.postgres.ast.PostgresConstant;
import lama.postgres.ast.PostgresExpression;
import lama.postgres.ast.PostgresFunction;
import lama.postgres.ast.PostgresInOperation;
import lama.postgres.ast.PostgresOrderByTerm;
import lama.postgres.ast.PostgresPOSIXRegularExpression;
import lama.postgres.ast.PostgresPostfixOperation;
import lama.postgres.ast.PostgresPostfixText;
import lama.postgres.ast.PostgresPrefixOperation;
import lama.postgres.ast.PostgresSelect;
import lama.postgres.ast.PostgresSimilarTo;

public class PostgresExpectedValueVisitor extends PostgresVisitor {

	private final StringBuilder sb = new StringBuilder();
	private int nrTabs = 0;

	private void print(PostgresExpression expr) {
		PostgresToStringVisitor v = new PostgresToStringVisitor();
		v.visit(expr);
		for (int i = 0; i < nrTabs; i++) {
			sb.append("\t");
		}
		sb.append(v.get());
		sb.append(" -- " + expr.getExpectedValue());
		sb.append("\n");
	}

	@Override
	public void visit(PostgresExpression expr) {
		nrTabs++;
		try {
			super.visit(expr);
		} catch (IgnoreMeException e) {

		}
		nrTabs--;
	}

	@Override
	public void visit(PostgresConstant constant) {
		print(constant);
	}

	@Override
	public void visit(PostgresPostfixOperation op) {
		print(op);
		visit(op.getExpression());
	}

	public String get() {
		return sb.toString();
	}

	@Override
	public void visit(PostgresColumnValue c) {
		print(c);
	}

	@Override
	public void visit(PostgresPrefixOperation op) {
		print(op);
		visit(op.getExpression());
	}

	@Override
	public void visit(PostgresSelect op) {
		visit(op.getWhereClause());
	}

	@Override
	public void visit(PostgresOrderByTerm op) {

	}

	@Override
	public void visit(PostgresFunction f) {
		print(f);
		for (int i = 0; i < f.getArguments().length; i++) {
			visit(f.getArguments()[i]);
		}
	}

	@Override
	public void visit(PostgresCastOperation cast) {
		print(cast);
		visit(cast.getExpression());
	}

	@Override
	public void visit(PostgresBinaryOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(PostgresBetweenOperation op) {
		print(op);
		visit(op.getExpr());
		visit(op.getLeft());
		visit(op.getRight());
	}


	@Override
	public void visit(PostgresInOperation op) {
		print(op);
		visit(op.getExpr());
		for (PostgresExpression right : op.getListElements()) {
			visit(right);
		}
	}

	@Override
	public void visit(PostgresPostfixText op) {
		print(op);
		visit(op.getExpr());
	}

	@Override
	public void visit(PostgresAggregate op) {
		print(op);
		visit(op.getExpr());
	}

	@Override
	public void visit(PostgresSimilarTo op) {
		print(op);
		visit(op.getString());
		visit(op.getSimilarTo());
		if (op.getEscapeCharacter() != null) {
			visit(op.getEscapeCharacter());
		}
	}
	
	@Override
	public void visit(PostgresPOSIXRegularExpression op) {
		print(op);
		visit(op.getString());
		visit(op.getRegex());
	}

	@Override
	public void visit(PostgresCollate op) {
		print(op);
		visit(op.getExpr());
	}

}
