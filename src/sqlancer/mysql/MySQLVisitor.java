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
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;

public abstract class MySQLVisitor {

	public abstract void visit(MySQLTableReference ref);
	
	public abstract void visit(MySQLConstant constant);

	public abstract void visit(MySQLColumnReference column);

	public abstract void visit(MySQLUnaryPrefixOperation column);

	public abstract void visit(MySQLUnaryPostfixOperation column);

	public abstract void visit(MySQLComputableFunction f);

	public abstract void visit(MySQLBinaryLogicalOperation op);

	public abstract void visit(MySQLSelect select);

	public abstract void visit(MySQLBinaryComparisonOperation op);

	public abstract void visit(MySQLCastOperation op);

	public abstract void visit(MySQLInOperation op);

	public abstract void visit(MySQLBinaryOperation op);

	public abstract void visit(MySQLOrderByTerm op);

	public abstract void visit(MySQLExists op);

	public abstract void visit(MySQLStringExpression op);
	
	public abstract void visit(MySQLBetweenOperation op);
	
	public abstract void visit(MySQLCollate collate);

	public void visit(MySQLExpression expr) {
		if (expr instanceof MySQLConstant) {
			visit((MySQLConstant) expr);
		} else if (expr instanceof MySQLColumnReference) {
			visit((MySQLColumnReference) expr);
		} else if (expr instanceof MySQLUnaryPrefixOperation) {
			visit((MySQLUnaryPrefixOperation) expr);
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
