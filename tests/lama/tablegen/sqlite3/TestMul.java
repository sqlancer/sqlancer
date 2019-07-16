package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation.BinaryOperator;

public class TestMul {
	
	@Test
	public void test0() {
		SQLite3Constant text = SQLite3TextConstant.createTextConstant("a");
		SQLite3Constant intVal = SQLite3Constant.createIntConstant(123);
		BinaryOperation mult = new SQLite3Expression.BinaryOperation(text, intVal, BinaryOperator.MULTIPLY);
		assertEquals(0, mult.getExpectedValue().asInt());
	}
	
	@Test
	public void test1() {
		SQLite3Constant text1 = SQLite3TextConstant.createTextConstant("-1");
		SQLite3Constant text2 = SQLite3TextConstant.createTextConstant("-1.60170686155407e+308");

		BinaryOperation mult = new SQLite3Expression.BinaryOperation(text1, text2, BinaryOperator.REMAINDER);
		assertEquals(0.0, mult.getExpectedValue().asDouble(), 0.00001);
	}
	
	@Test
	public void testNan() {
		SQLite3Constant inf = SQLite3Constant.createRealConstant(Double.POSITIVE_INFINITY);
		SQLite3Constant zero = SQLite3Constant.createRealConstant(0.0);
		assertTrue(new SQLite3Expression.BinaryOperation(inf, zero, BinaryOperator.MULTIPLY).getExpectedValue().isNull());
	}
	
	@Test
	public void testOverflow() {
		SQLite3Constant min = SQLite3Constant.createIntConstant(Long.MIN_VALUE);
		SQLite3Constant minusOne = SQLite3Constant.createIntConstant(-1);
		var mult = new BinaryOperation(min, minusOne, BinaryOperator.DIVIDE);
		assertEquals(9.22337203685478e+18, mult.getExpectedValue().asDouble(), 0.0001);
	}

}
