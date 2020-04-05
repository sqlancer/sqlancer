package sqlancer.mysql;

import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLCollate;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLOrderByTerm;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLStringExpression;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;

public interface MySQLVisitor {

	public void visit(MySQLTableReference ref);
	
	public void visit(MySQLConstant constant);

	public void visit(MySQLColumnReference column);

	public void visit(MySQLUnaryPostfixOperation column);

	public void visit(MySQLComputableFunction f);

	public void visit(MySQLBinaryLogicalOperation op);

	public void visit(MySQLSelect select);

	public void visit(MySQLBinaryComparisonOperation op);

	public void visit(MySQLCastOperation op);

	public void visit(MySQLInOperation op);

	public void visit(MySQLBinaryOperation op);

	public void visit(MySQLOrderByTerm op);

	public void visit(MySQLExists op);

	public void visit(MySQLStringExpression op);
	
	public void visit(MySQLBetweenOperation op);
	
	public void visit(MySQLCollate collate);

	public default void visit(MySQLExpression expr) {
		if (expr instanceof MySQLConstant) {
			visit((MySQLConstant) expr);
		} else if (expr instanceof MySQLColumnReference) {
			visit((MySQLColumnReference) expr);
		} else if (expr instanceof MySQLUnaryPostfixOperation) {
			visit((MySQLUnaryPostfixOperation) expr);
		} else if (expr instanceof MySQLComputableFunction) {
			visit((MySQLComputableFunction) expr);
		} else if (expr instanceof MySQLBinaryLogicalOperation) {
			visit((MySQLBinaryLogicalOperation) expr);
		} else if (expr instanceof MySQLSelect) {
			visit((MySQLSelect) expr);
		} else if (expr instanceof MySQLBinaryComparisonOperation) {
			visit((MySQLBinaryComparisonOperation) expr);
		} else if (expr instanceof MySQLCastOperation) {
			visit((MySQLCastOperation) expr);
		} else if (expr instanceof MySQLInOperation) {
			visit((MySQLInOperation) expr);
		} else if (expr instanceof MySQLBinaryOperation) {
			visit((MySQLBinaryOperation) expr);
		} else if (expr instanceof MySQLOrderByTerm) {
			visit((MySQLOrderByTerm) expr);
		} else if (expr instanceof MySQLExists) {
			visit((MySQLExists) expr);
		} else if (expr instanceof MySQLStringExpression) {
			visit((MySQLStringExpression) expr);
		} else if (expr instanceof MySQLBetweenOperation) {
			visit((MySQLBetweenOperation) expr);
		} else if (expr instanceof MySQLTableReference) {
			visit((MySQLTableReference) expr);
		} else if (expr instanceof MySQLCollate) {
			visit((MySQLCollate) expr);
		}
		
		else {
			throw new AssertionError(expr);
		}
	}

	public static String asString(MySQLExpression expr) {
		MySQLToStringVisitor visitor = new MySQLToStringVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

	public static String asExpectedValues(MySQLExpression expr) {
		MySQLExpectedValueVisitor visitor = new MySQLExpectedValueVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

}
