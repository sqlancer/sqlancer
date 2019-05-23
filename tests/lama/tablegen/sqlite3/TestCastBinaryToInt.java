package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.gen.SQLite3Cast;

class TestCastBinaryToInt {

	@Test
	void test1() {
		assertBinaryCastToInt("dbb25259", 0);
	}

	@Test
	void test2() {
		assertBinaryCastToInt("d9a3", 0);
	}

	void assertBinaryCastToInt(String val, long expectedLong) {
		SQLite3Constant c = SQLite3Constant.createBinaryConstant(val);
		SQLite3Constant intVal = SQLite3Cast.castToInt(c);
		assertEquals(intVal.asInt(), expectedLong);
	}

}
