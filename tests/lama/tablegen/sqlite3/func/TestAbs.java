package lama.tablegen.sqlite3.func;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Function.ComputableFunction;

public class TestAbs {

	@Test // SELECT ABS(0); -- 0
	public void testZero() {
		SQLite3Constant zero = SQLite3Constant.createIntConstant(0);
		ComputableFunction f = ComputableFunction.ABS;
		assertEquals(0, f.apply(zero).asInt());
	}

	@Test // SELECT ABS(-1); -- 1
	public void testMinusOne() {
		SQLite3Constant minusOne = SQLite3Constant.createIntConstant(-1);
		ComputableFunction f = ComputableFunction.ABS;
		assertEquals(1, f.apply(minusOne).asInt());
	}

	@Test // SELECT ABS(-9223372036854775807); -- 9223372036854775807
	public void testMin() {
		SQLite3Constant minusOne = SQLite3Constant.createIntConstant(-Long.MAX_VALUE);
		ComputableFunction f = ComputableFunction.ABS;
		assertEquals(Long.MAX_VALUE, f.apply(minusOne).asInt());
	}

	@Test // SELECT ABS(-123.456); -- 123.456
	public void testReal() {
		SQLite3Constant realVal = SQLite3Constant.createRealConstant(-123.456);
		assertEquals(123.456, ComputableFunction.ABS.apply(realVal).asDouble(), 0.0000000001);
	}

	@Test // SELECT ABS(NULL); -- NULL
	public void testNull() {
		assertTrue(ComputableFunction.ABS.apply(SQLite3Constant.createNullConstant()).isNull());
	}

	@Test // SELECT ABS('-3.5');
	public void testString() {
		assertEquals(3.5, ComputableFunction.ABS.apply(SQLite3Constant.createTextConstant("-3.5")).asDouble(),
				0.0000000001);
	}
	
	@Test // SELECT ABS('x'); -- 0.0
	public void testString1() {
		assertEquals(0.0, ComputableFunction.ABS.apply(SQLite3Constant.createTextConstant("x")).asDouble(),
				0.0000000001);
	}
	
	@Test // SELECT ABS('1147056737'); -- 1147056737
	public void testString2() {
		assertEquals(1147056737.0, ComputableFunction.ABS.apply(SQLite3Constant.createTextConstant("1147056737")).asDouble(), 0.0001);
	}

}
