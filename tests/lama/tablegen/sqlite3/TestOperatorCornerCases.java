package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.Cast;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Expression.UnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation.BinaryOperator;
import lama.sqlite3.ast.SQLite3Expression.UnaryOperation.UnaryOperator;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column;

public class TestOperatorCornerCases {
	
	// unary
	
	@Test
	public void testUnaryMinValueMinus() {
		SQLite3Constant val = SQLite3Constant.createIntConstant(-9223372036854775808L);
		UnaryOperation op = new UnaryOperation(UnaryOperator.MINUS, val);
		assertEquals(op.getExpectedValue().asDouble(), -(double) Long.MIN_VALUE);
	}
	
	@Test
	public void testNotZero() {
		SQLite3Constant val = SQLite3Constant.createIntConstant(0);
		UnaryOperation op = new UnaryOperation(UnaryOperator.NOT, val);
		assertEquals(op.getExpectedValue().asInt(), 1);
	}
	
	@Test
	public void testMinusReal() {
		SQLite3Expression val = new SQLite3Expression.ColumnName(new Column("c", SQLite3DataType.TEXT, false, false), SQLite3Constant.createTextConstant("1562730931.0"));
		UnaryOperation op = new UnaryOperation(UnaryOperator.MINUS, val);
		assertEquals(op.getExpectedValue().asDouble(), -1562730931.0);
	}
	

	// binary
	
	@Test
	public void testEqualsTextInt() {
		SQLite3Expression val = new SQLite3Expression.ColumnName(new Column("c", SQLite3DataType.TEXT, false, false), SQLite3Constant.createTextConstant("2126895850"));
		assertEquals(1, val.getExpectedValue().applyEquals(val.getExpectedValue()).asInt());
	}
	
	// affinity
	@Test
	public void testApplyTextAffinity() {
		SQLite3Constant intVal = SQLite3Constant.createIntConstant(5);
		SQLite3Expression textWithAffinityVal = new Cast(new TypeLiteral(TypeLiteral.Type.TEXT), SQLite3Constant.createTextConstant("5"));
		BinaryOperation op = new SQLite3Expression.BinaryOperation(intVal, textWithAffinityVal, BinaryOperator.EQUALS);
		assertEquals(1, op.getExpectedValue().asInt());
	}
	
}
