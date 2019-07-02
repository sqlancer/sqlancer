package postgres;

import lama.postgres.ast.PostgresComputableFunction;
import postgres.ast.PostgresBinaryComparisonOperation;
import postgres.ast.PostgresBinaryLogicalOperation;
import postgres.ast.PostgresCastOperation;
import postgres.ast.PostgresColumnValue;
import postgres.ast.PostgresConstant;
import postgres.ast.PostgresExpression;
import postgres.ast.PostgresLikeOperation;
import postgres.ast.PostgresOrderByTerm;
import postgres.ast.PostgresPostfixOperation;
import postgres.ast.PostgresPrefixOperation;
import postgres.ast.PostgresSelect;

public abstract class PostgresVisitor {
	
	public abstract void visit(PostgresConstant constant);
	
	public abstract void visit(PostgresPostfixOperation op);
	
	public abstract void visit(PostgresColumnValue c);
	
	public abstract void visit(PostgresPrefixOperation op);
	
	public abstract void visit(PostgresBinaryLogicalOperation op);
	
	public abstract void visit(PostgresSelect op);

	public abstract void visit(PostgresOrderByTerm op);
	
	public abstract void visit(PostgresBinaryComparisonOperation op);
	
	public abstract void visit(PostgresComputableFunction f);
	
	public abstract void visit(PostgresCastOperation cast);

	public abstract void visit(PostgresLikeOperation op);
	
	
	public void visit(PostgresExpression expression) {
		if (expression instanceof PostgresConstant) {
			visit((PostgresConstant) expression);
		} else if (expression instanceof PostgresPostfixOperation) {
			visit((PostgresPostfixOperation) expression);
		} else if (expression instanceof PostgresColumnValue) {
			visit((PostgresColumnValue) expression);
		} else if (expression instanceof PostgresPrefixOperation) {
			visit((PostgresPrefixOperation) expression);
		} else if (expression instanceof PostgresBinaryLogicalOperation) {
			visit((PostgresBinaryLogicalOperation) expression);
		} else if (expression instanceof PostgresSelect) {
			visit((PostgresSelect) expression);
		} else if (expression instanceof PostgresOrderByTerm) {
			visit((PostgresOrderByTerm) expression);
		} else if (expression instanceof PostgresBinaryComparisonOperation) {
			visit((PostgresBinaryComparisonOperation) expression);
		} else if (expression instanceof PostgresComputableFunction) {
			visit((PostgresComputableFunction) expression);
		} else if (expression instanceof PostgresCastOperation) {
			visit((PostgresCastOperation) expression);
		} else if (expression instanceof PostgresLikeOperation) {
			visit((PostgresLikeOperation) expression);
		} else {
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
