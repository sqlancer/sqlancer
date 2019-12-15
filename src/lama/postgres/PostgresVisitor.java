package lama.postgres;

import lama.postgres.ast.PostgresAggregate;
import lama.postgres.ast.PostgresBetweenOperation;
import lama.postgres.ast.PostgresBinaryOperation;
import lama.postgres.ast.PostgresBinaryRangeOperation;
import lama.postgres.ast.PostgresCastOperation;
import lama.postgres.ast.PostgresCollate;
import lama.postgres.ast.PostgresColumnValue;
import lama.postgres.ast.PostgresConcatOperation;
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

public abstract class PostgresVisitor {

	public abstract void visit(PostgresConstant constant);

	public abstract void visit(PostgresPostfixOperation op);

	public abstract void visit(PostgresColumnValue c);

	public abstract void visit(PostgresPrefixOperation op);

	public abstract void visit(PostgresSelect op);

	public abstract void visit(PostgresOrderByTerm op);

	public abstract void visit(PostgresFunction f);

	public abstract void visit(PostgresCastOperation cast);

	public abstract void visit(PostgresBinaryOperation op);

	public abstract void visit(PostgresBetweenOperation op);

	public abstract void visit(PostgresInOperation op);

	public abstract void visit(PostgresPostfixText op);

	public abstract void visit(PostgresAggregate op);

	public abstract void visit(PostgresSimilarTo op);

	public abstract void visit(PostgresCollate op);

	public abstract void visit(PostgresPOSIXRegularExpression op);

	public void visit(PostgresExpression expression) {
		if (expression instanceof PostgresConstant) {
			visit((PostgresConstant) expression);
		} else if (expression instanceof PostgresPostfixOperation) {
			visit((PostgresPostfixOperation) expression);
		} else if (expression instanceof PostgresColumnValue) {
			visit((PostgresColumnValue) expression);
		} else if (expression instanceof PostgresPrefixOperation) {
			visit((PostgresPrefixOperation) expression);
		} else if (expression instanceof PostgresSelect) {
			visit((PostgresSelect) expression);
		} else if (expression instanceof PostgresOrderByTerm) {
			visit((PostgresOrderByTerm) expression);
		} else if (expression instanceof PostgresFunction) {
			visit((PostgresFunction) expression);
		} else if (expression instanceof PostgresCastOperation) {
			visit((PostgresCastOperation) expression);
		} else if (expression instanceof PostgresBetweenOperation) {
			visit((PostgresBetweenOperation) expression);
		} else if (expression instanceof PostgresConcatOperation) {
			visit((PostgresConcatOperation) expression);
		} else if (expression instanceof PostgresInOperation) {
			visit((PostgresInOperation) expression);
		} else if (expression instanceof PostgresAggregate) {
			visit((PostgresAggregate) expression);
		} else if (expression instanceof PostgresPostfixText) {
			visit((PostgresPostfixText) expression);
		} else if (expression instanceof PostgresSimilarTo) {
			visit((PostgresSimilarTo) expression);
		} else if (expression instanceof PostgresPOSIXRegularExpression) {
			visit((PostgresPOSIXRegularExpression) expression);
		} else if (expression instanceof PostgresCollate) {
			visit((PostgresCollate) expression);
		} else if (expression instanceof PostgresBinaryOperation) {
			visit((PostgresBinaryOperation) expression);
		}

		else {
			throw new AssertionError(expression);
		}
	}

	public static String asString(PostgresExpression expr) {
		PostgresToStringVisitor visitor = new PostgresToStringVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

	public static String asExpectedValues(PostgresExpression expr) {
		PostgresExpectedValueVisitor v = new PostgresExpectedValueVisitor();
		v.visit(expr);
		return v.get();
	}

}
