package lama.mysql;

import lama.mysql.ast.MySQLBinaryComparisonOperation;
import lama.mysql.ast.MySQLBinaryLogicalOperation;
import lama.mysql.ast.MySQLCastOperation;
import lama.mysql.ast.MySQLColumnValue;
import lama.mysql.ast.MySQLComputableFunction;
import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLExpression;
import lama.mysql.ast.MySQLSelect;
import lama.mysql.ast.MySQLUnaryPrefixOperation;
import lama.mysql.ast.MySQLUnaryPostfixOperator;

public abstract class MySQLVisitor {

	public abstract void visit(MySQLConstant constant);

	public abstract void visit(MySQLColumnValue column);

	public abstract void visit(MySQLUnaryPrefixOperation column);

	public abstract void visit(MySQLUnaryPostfixOperator column);

	public abstract void visit(MySQLComputableFunction f);

	public abstract void visit(MySQLBinaryLogicalOperation op);

	public abstract void visit(MySQLSelect select);

	public abstract void visit(MySQLBinaryComparisonOperation op);
	
	public abstract void visit(MySQLCastOperation op);

	public void visit(MySQLExpression expr) {
		if (expr instanceof MySQLConstant) {
			visit((MySQLConstant) expr);
		} else if (expr instanceof MySQLColumnValue) {
			visit((MySQLColumnValue) expr);
		} else if (expr instanceof MySQLUnaryPrefixOperation) {
			visit((MySQLUnaryPrefixOperation) expr);
		} else if (expr instanceof MySQLUnaryPostfixOperator) {
			visit((MySQLUnaryPostfixOperator) expr);
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
		} else {
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
