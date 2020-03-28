package sqlancer.postgres;

import sqlancer.postgres.ast.PostgresAggregate;
import sqlancer.postgres.ast.PostgresBetweenOperation;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresCollate;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresFunction;
import sqlancer.postgres.ast.PostgresInOperation;
import sqlancer.postgres.ast.PostgresOrderByTerm;
import sqlancer.postgres.ast.PostgresPOSIXRegularExpression;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixText;
import sqlancer.postgres.ast.PostgresPrefixOperation;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSimilarTo;

public interface PostgresVisitor {

	public abstract void visit(PostgresConstant constant);

	public abstract void visit(PostgresPostfixOperation op);

	public abstract void visit(PostgresColumnValue c);

	public abstract void visit(PostgresPrefixOperation op);

	public abstract void visit(PostgresSelect op);

	public abstract void visit(PostgresOrderByTerm op);

	public abstract void visit(PostgresFunction f);

	public abstract void visit(PostgresCastOperation cast);

	public abstract void visit(PostgresBetweenOperation op);

	public abstract void visit(PostgresInOperation op);

	public abstract void visit(PostgresPostfixText op);

	public abstract void visit(PostgresAggregate op);

	public abstract void visit(PostgresSimilarTo op);

	public abstract void visit(PostgresCollate op);

	public abstract void visit(PostgresPOSIXRegularExpression op);
	
	public abstract void visit(PostgresFromTable from);

	public default void visit(PostgresExpression expression) {
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
		} else if (expression instanceof PostgresFromTable) {
			visit((PostgresFromTable) expression);
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
