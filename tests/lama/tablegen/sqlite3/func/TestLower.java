package lama.tablegen.sqlite3.func;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Function.ComputableFunction;

public class TestLower {

	@Test
	public void testString() {
		SQLite3Constant umlaut = SQLite3Constant.createTextConstant("AbZ");
		assertEquals("abz", ComputableFunction.LOWER.apply(umlaut).asString());
	}

	@Test
	public void testGermanUmlaut() {
		SQLite3Constant umlaut = SQLite3Constant.createTextConstant("Ö");
		assertEquals("Ö", ComputableFunction.LOWER.apply(umlaut).asString());
	}

}
