package lama.tablegen.sqlite3.cast;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.gen.SQLite3Cast;

public class TestCastToBlob {
	
	
	@Test
	public void testNull() {
		SQLite3Constant nullVal = SQLite3Constant.createNullConstant();
		SQLite3Constant castNullVal = SQLite3Cast.castToBlob(nullVal);
		assertTrue(castNullVal.isNull());
	}
	
	@Test
	public void testEmptyString() {
		SQLite3Constant emptyBinary = SQLite3Constant.createTextConstant("");
		SQLite3Constant castVal = SQLite3Cast.castToBlob(emptyBinary);
		assertArrayEquals(new byte[0], castVal.asBinary());
	}
	
	
	@Test
	public void testString1() {
		assertCastStringToBlob("0x12", "0x12");
	}
	
	@Test
	public void testString2() {
		assertCastStringToBlob("123", "x'313233'");
	}
	
	void assertCastStringToBlob(String val, String expectedBlob) {
		SQLite3Constant c = SQLite3Constant.createTextConstant(val);
		SQLite3Constant binVal = SQLite3Cast.castToBlob(c);
		assertEquals(binVal.toString(), expectedBlob);
	}


}
