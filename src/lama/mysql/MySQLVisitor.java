package lama.mysql;

import lama.mysql.ast.MySQLBinaryLogicalOperation;
import lama.mysql.ast.MySQLColumnValue;
import lama.mysql.ast.MySQLComputableFunction;
import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLExpression;
import lama.mysql.ast.MySQLUnaryNotOperator;
import lama.mysql.ast.MySQLUnaryPostfixOperator;

public abstract class MySQLVisitor {

	public abstract void visit(MySQLConstant constant);

	public abstract void visit(MySQLColumnValue column);

	public abstract void visit(MySQLUnaryNotOperator column);

	public abstract void visit(MySQLUnaryPostfixOperator column);

	public abstract void visit(MySQLComputableFunction f);
	
	public abstract void visit(MySQLBinaryLogicalOperation op);

	public void visit(MySQLExpression expr) {
		if (expr instanceof MySQLConstant) {
			visit((MySQLConstant) expr);
		} else if (expr instanceof MySQLColumnValue) {
			visit((MySQLColumnValue) expr);
		} else if (expr instanceof MySQLUnaryNotOperator) {
			visit((MySQLUnaryNotOperator) expr);
		} else if (expr instanceof MySQLUnaryPostfixOperator) {
			visit((MySQLUnaryPostfixOperator) expr);
		} else if (expr instanceof MySQLComputableFunction) {
			visit((MySQLComputableFunction) expr);
		} else if (expr instanceof MySQLBinaryLogicalOperation) {
			visit((MySQLBinaryLogicalOperation) expr);
		} else {
			throw new AssertionError(expr);
		}
	}

}
