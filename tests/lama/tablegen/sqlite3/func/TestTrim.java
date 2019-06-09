package lama.tablegen.sqlite3.func;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Function.ComputableFunction;

public class TestTrim {

	@Test
	public void testInt() {
		SQLite3Constant val = SQLite3Constant.createIntConstant(5);
		assertEquals("5", ComputableFunction.TRIM.apply(val).asString());
	}

	@Test
	public void testString1() {
		SQLite3Constant val = SQLite3Constant.createTextConstant(" 5 ");
		assertEquals("5", ComputableFunction.TRIM.apply(val).asString());
	}

	// two args

	@Test
	public void testSameString() {
		SQLite3Constant arg = SQLite3Constant.createTextConstant("asdf");
		assertEquals("", ComputableFunction.TRIM_TWO_ARGS.apply(arg, arg).asString());
	}

	@Test
	public void testStringString() {
		SQLite3Constant arg1 = SQLite3Constant.createTextConstant("hello world!");
		SQLite3Constant arg2 = SQLite3Constant.createTextConstant("!dhle");
		assertEquals("o wor", ComputableFunction.TRIM_TWO_ARGS.apply(arg1, arg2).asString());
	}

	@Test
	public void testStringInt() {
		SQLite3Constant arg = SQLite3Constant.createIntConstant(5);
		assertEquals("", ComputableFunction.TRIM_TWO_ARGS.apply(arg, arg).asString());
	}

	@Test
	public void testIntInt() {
		SQLite3Constant arg1 = SQLite3Constant.createIntConstant(6);
		SQLite3Constant arg2 = SQLite3Constant.createIntConstant(5);
		assertEquals("6", ComputableFunction.TRIM_TWO_ARGS.apply(arg1, arg2).asString());
	}

}
