package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;

class TestApplyNumericAffinity {

	@Test
	void minusZero() {
		SQLite3Constant minusZero = SQLite3TextConstant.createTextConstant("-0.0").applyNumericAffinity();
		assertEquals(minusZero.asInt(), 0);
	}

	@Test
	void plusSeven() {
		SQLite3Constant minusZero = SQLite3TextConstant.createTextConstant("+7").applyNumericAffinity();
		assertEquals(minusZero.asInt(), 7);
	}

	@Test
	void zeroPointZero() {
		SQLite3Constant minusZero = SQLite3TextConstant.createTextConstant("0.0").applyNumericAffinity();
		assertEquals(minusZero.asInt(), 0);
	}

	@Test
	void minusZero2() {
		SQLite3Constant minusZero = SQLite3TextConstant.createTextConstant("-0").applyNumericAffinity();
		assertEquals(minusZero.asInt(), 0);
	}

	@Test
	void fivePointZero() {
		SQLite3Constant val = SQLite3TextConstant.createTextConstant("5.0").applyNumericAffinity();
		assertEquals(val.asInt(), 5);
	}

	@Test
	void fivePoint() {
		SQLite3Constant val = SQLite3TextConstant.createTextConstant("5.").applyNumericAffinity();
		assertEquals(val.asInt(), 5);
	}

	@Test
	void pointFive() {
		SQLite3Constant minusZero = SQLite3TextConstant.createTextConstant(".5").applyNumericAffinity();
		assertEquals(minusZero.asDouble(), 0.5);
	}

	@Test
	void pointZero() {
		SQLite3Constant pointZero = SQLite3TextConstant.createTextConstant(".0").applyNumericAffinity();
		assertEquals(pointZero.asInt(), 0);
	}

	@Test
	void zeroZero() {
		SQLite3Constant minusZero = SQLite3TextConstant.createTextConstant("00").applyNumericAffinity();
		assertEquals(minusZero.asInt(), 0);
	}

	@Test
	void testZeroSeven() {
		SQLite3Constant minusZero = SQLite3TextConstant.createTextConstant("07").applyNumericAffinity();
		assertEquals(minusZero.asInt(), 7);
	}

	@Test
	void testSpace() {
		SQLite3Constant minusZero = SQLite3TextConstant.createTextConstant("8 ").applyNumericAffinity();
		assertEquals(minusZero.asInt(), 8);
	}

}
