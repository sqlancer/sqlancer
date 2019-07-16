package lama.postgres;

import lama.IgnoreMeException;
import lama.postgres.ast.PostgresBetweenOperation;
import lama.postgres.ast.PostgresBinaryArithmeticOperation;
import lama.postgres.ast.PostgresBinaryComparisonOperation;
import lama.postgres.ast.PostgresBinaryLogicalOperation;
import lama.postgres.ast.PostgresCastOperation;
import lama.postgres.ast.PostgresColumnValue;
import lama.postgres.ast.PostgresComputableFunction;
import lama.postgres.ast.PostgresConcatOperation;
import lama.postgres.ast.PostgresConstant;
import lama.postgres.ast.PostgresExpression;
import lama.postgres.ast.PostgresInOperation;
import lama.postgres.ast.PostgresLikeOperation;
import lama.postgres.ast.PostgresOrderByTerm;
import lama.postgres.ast.PostgresPostfixOperation;
import lama.postgres.ast.PostgresPrefixOperation;
import lama.postgres.ast.PostgresSelect;

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
	public void visit(PostgresBinaryLogicalOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(PostgresSelect op) {
		visit(op.getWhereClause());
	}

	@Override
	public void visit(PostgresOrderByTerm op) {

	}

	@Override
	public void visit(PostgresBinaryComparisonOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(PostgresComputableFunction f) {
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
	public void visit(PostgresLikeOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(PostgresBinaryArithmeticOperation op) {
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
	public void visit(PostgresConcatOperation op) {
		print(op);
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

}
