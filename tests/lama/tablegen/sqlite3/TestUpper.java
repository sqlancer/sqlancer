package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Function.ComputableFunction;

public class TestUpper {

	@Test
	public void testString() {
		SQLite3Constant umlaut = SQLite3Constant.createTextConstant("aBz");
		assertEquals("ABZ", ComputableFunction.UPPER.apply(umlaut).asString());
	}

	@Test
	public void testGermanUmlaut() {
		SQLite3Constant umlaut = SQLite3Constant.createTextConstant("ö");
		assertEquals("ö", ComputableFunction.UPPER.apply(umlaut).asString());
	}

}
