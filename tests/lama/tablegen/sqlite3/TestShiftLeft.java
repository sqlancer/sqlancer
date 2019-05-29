package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3IntConstant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation.BinaryOperator;

public class TestShiftLeft {

	SQLite3Constant intZero = SQLite3Constant.createIntConstant(0);
	SQLite3Constant intOne = SQLite3TextConstant.createIntConstant(1);
	SQLite3Constant intFifty = SQLite3TextConstant.createIntConstant(50);
	SQLite3Constant intHundred = SQLite3IntConstant.createIntConstant(100);
	SQLite3Constant intMax = SQLite3IntConstant.createIntConstant(Long.MAX_VALUE);
	SQLite3Constant minusThree = SQLite3Constant.createIntConstant(-3);
	SQLite3Constant intMinusHundred = SQLite3Constant.createIntConstant(-100);
	SQLite3Constant intMinusOne = SQLite3Constant.createIntConstant(-1);
	SQLite3Constant nullValue = SQLite3Constant.createNullConstant();

	@Test
	public void test0() { // SELECT 1 << 50; -- 1125899906842624
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intOne, intFifty, BinaryOperator.SHIFT_LEFT);
		assertEquals(1125899906842624L, shift.getExpectedValue().asInt());
	}

	@Test
	public void test1() { // SELECT 1 << 9223372036854775807; -- 0
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intOne, intMax, BinaryOperator.SHIFT_LEFT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

	@Test
	public void test2() { // SELECT 0 << 1; -- 0
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intZero, intOne, BinaryOperator.SHIFT_LEFT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

	@Test
	public void test3() { // SELECT -3 << 1; -- -6
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(minusThree, intOne, BinaryOperator.SHIFT_LEFT);
		assertEquals(-6, shift.getExpectedValue().asInt());
	}

	@Test
	public void test4() { // SELECT -3 << 100; -- -3377699720527872
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(minusThree, intFifty, BinaryOperator.SHIFT_LEFT);
		assertEquals(-3377699720527872L, shift.getExpectedValue().asInt());
	}

	@Test
	public void test5() { // SELECT 50 << -3; -- 6
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intFifty, minusThree, BinaryOperator.SHIFT_LEFT);
		assertEquals(6, shift.getExpectedValue().asInt());
	}

	@Test
	public void test6() { // SELECT 50 << -100; -- 6
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intFifty, intMinusHundred,
				BinaryOperator.SHIFT_LEFT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

	@Test
	public void test7() { // SELECT 50 << NULL; -- NULL
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intFifty, nullValue, BinaryOperator.SHIFT_LEFT);
		assertTrue(shift.getExpectedValue().isNull());
	}

	@Test
	public void test8() { // SELECT NULL << 1; -- NULL
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(nullValue, intOne, BinaryOperator.SHIFT_LEFT);
		assertTrue(shift.getExpectedValue().isNull());
	}

	@Test
	public void test9() { // SELECT -3 << -1; -- -2
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(minusThree, intMinusOne,
				BinaryOperator.SHIFT_LEFT);
		assertEquals(-2, shift.getExpectedValue().asInt());
	}

	@Test
	public void test10() { // SELECT -1285817674 >> -1792583644; -- -1
		SQLite3Constant largeNegative1 = SQLite3Constant.createIntConstant(-1285817674);
		SQLite3Constant largeNegative2 = SQLite3Constant.createIntConstant(-1792583644);
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(largeNegative1, largeNegative2,
				BinaryOperator.SHIFT_RIGHT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

	// right shift

	@Test
	public void testRightShift0() { // SELECT 1 >> 50; -- 0
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intOne, intFifty, BinaryOperator.SHIFT_RIGHT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

	@Test
	public void testRightShift1() { // SELECT 1 >> 9223372036854775807; -- 0
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intOne, intMax, BinaryOperator.SHIFT_RIGHT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

	@Test
	public void testRightShift2() { // SELECT 0 >> 1; -- 0
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intZero, intOne, BinaryOperator.SHIFT_RIGHT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

	@Test
	public void testRightShift3() { // SELECT -3 >> 1; -- -2
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(minusThree, intOne, BinaryOperator.SHIFT_RIGHT);
		assertEquals(-2, shift.getExpectedValue().asInt());
	}

	@Test
	public void testRightShift4() { // SELECT -3 >> 100; -- -1
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(minusThree, intFifty, BinaryOperator.SHIFT_RIGHT);
		assertEquals(-1, shift.getExpectedValue().asInt());
	}

	@Test
	public void testRightShift5() { // SELECT 50 >> -3; -- 400
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intFifty, minusThree, BinaryOperator.SHIFT_RIGHT);
		assertEquals(400, shift.getExpectedValue().asInt());
	}

	@Test
	public void testRightShift6() { // SELECT 50 >> -100; -- 0
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intFifty, intMinusHundred,
				BinaryOperator.SHIFT_RIGHT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

	@Test
	public void testRightShift7() { // SELECT 50 >> NULL; -- NULL
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(intFifty, nullValue, BinaryOperator.SHIFT_RIGHT);
		assertTrue(shift.getExpectedValue().isNull());
	}

	@Test
	public void testRightShift8() { // SELECT NULL >> 1; -- NULL
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(nullValue, intOne, BinaryOperator.SHIFT_RIGHT);
		assertTrue(shift.getExpectedValue().isNull());
	}

	@Test
	public void testRightShift9() { // SELECT -3 >> -1; -- -6
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(minusThree, intMinusOne,
				BinaryOperator.SHIFT_RIGHT);
		assertEquals(-6, shift.getExpectedValue().asInt());
	}

	@Test
	public void testRightShift10() { // SELECT -1285817674 >> -1792583644; -- 0
		SQLite3Constant largeNegative1 = SQLite3Constant.createIntConstant(-1285817674);
		SQLite3Constant largeNegative2 = SQLite3Constant.createIntConstant(-1792583644);
		BinaryOperation shift = new SQLite3Expression.BinaryOperation(largeNegative1, largeNegative2,
				BinaryOperator.SHIFT_RIGHT);
		assertEquals(0, shift.getExpectedValue().asInt());
	}

}
