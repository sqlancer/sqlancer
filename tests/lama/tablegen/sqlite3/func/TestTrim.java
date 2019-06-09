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

}
