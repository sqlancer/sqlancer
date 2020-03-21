package sqlancer.mysql;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLColumnValue;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLOrderByTerm;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLStringExpression;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperator;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresToStringVisitor;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

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

	public abstract void visit(MySQLInOperation op);

	public abstract void visit(MySQLBinaryOperation op);

	public abstract void visit(MySQLOrderByTerm op);

	public abstract void visit(MySQLExists op);

	public abstract void visit(MySQLStringExpression op);
	
	public abstract void visit(MySQLBetweenOperation op);

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

	public static String getExpressionAsString(Randomly r, PostgresDataType type) {
		PostgresExpression expression = PostgresExpressionGenerator.generateExpression(r, type);
		PostgresToStringVisitor visitor = new PostgresToStringVisitor();
		visitor.visit(expression);
		return visitor.get();
	}
	
	public static String getExpressionAsString(Randomly r, PostgresDataType type, List<PostgresColumn> columns) {
		PostgresExpression expression = PostgresExpressionGenerator.generateExpression(r, columns, type);
		PostgresToStringVisitor visitor = new PostgresToStringVisitor();
		visitor.visit(expression);
		return visitor.get();
	}

}
