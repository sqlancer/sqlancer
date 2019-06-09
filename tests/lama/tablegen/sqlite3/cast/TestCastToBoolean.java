package lama.tablegen.sqlite3.cast;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;

public class TestCastToBoolean {
	
	@Test
	void nan() {
		SQLite3Constant text = SQLite3Constant.createTextConstant("NaN");
		assertEquals(text.castToBoolean().asInt(), 0);
	}

}
