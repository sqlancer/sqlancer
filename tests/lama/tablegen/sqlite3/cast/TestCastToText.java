package lama.tablegen.sqlite3.cast;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.gen.SQLite3Cast;

public class TestCastToText {
	
	@Test
	void test0() {
		assertBinaryCastToText("3dca", "=�");
	}
	
	@Test
	void test1() {
		assertBinaryCastToText("7e0fa8", "~�");
	}
	
	@Test
	void test2() {
		assertBinaryCastToText("a4ee", "��");
	}
	
	@Test
	void test3() {
		assertBinaryCastToText("2D8A", "-�");
	}


	void assertBinaryCastToText(String val, String expected) {
		SQLite3Constant c = SQLite3Constant.createBinaryConstant(val);
		SQLite3Constant intVal = SQLite3Cast.castToText(c);
		assertEquals(intVal.asString(), expected);
	}
	
	@Test
	void testString1() {
		assertRealCastToText(1562730931.0, "1562730931.0");
	}
	
	@Test
	void testString2() {
		assertRealCastToText(1.834665208E9, "1834665208.0");
	}
	
	@Test
	void testString3() {
		assertRealCastToText(-1.5, "-1.5");
	}
	
	@Test
	void testString4() {
		assertRealCastToText(0.8205349286718593, "0.8205349286718593");
	}
	
	@Test
	void testString5() {
		assertRealCastToText(-0.6792529217385632, "-0.679252921738563");
	}
	
	@Test
	void testString6() {
		assertRealCastToText(0.6918798430590762, "0.691879843059076");
	}
	
	@Test
	void testString7() {
		assertRealCastToText(0.021848023722833787, "0.0218480237228338");
	}
	
	@Test
	void testString8() {
		assertRealCastToText(-1.6391052705683897E308, "-1.63910527056839e+308");
	}
	
	void assertRealCastToText(double val, String expected) {
		SQLite3Constant c = SQLite3Constant.createRealConstant(val);
		SQLite3Constant intVal = SQLite3Cast.castToText(c);
		assertEquals(expected, intVal.asString());
	}
	
}
