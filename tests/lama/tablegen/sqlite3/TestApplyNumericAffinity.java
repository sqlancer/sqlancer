package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;

class TestApplyNumericAffinity {

	// zero tests

	@Test
	void minusZero() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("-0.0").applyNumericAffinity();
		assertEquals(value.asInt(), 0);
	}

	@Test
	void zeroPointZero() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("0.0").applyNumericAffinity();
		assertEquals(value.asInt(), 0);
	}

	@Test
	void pointZero() {
		SQLite3Constant pointZero = SQLite3TextConstant.createTextConstant(".0").applyNumericAffinity();
		assertEquals(pointZero.asInt(), 0);
	}

	@Test
	void minusZero2() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("-0").applyNumericAffinity();
		assertEquals(value.asInt(), 0);
	}

	@Test
	void zeroZero() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("00").applyNumericAffinity();
		assertEquals(value.asInt(), 0);
	}

	@Test
	void testLongZero() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("-00000").applyNumericAffinity();
		assertEquals(value.asInt(), 0);
	}

	@Test
	void testZeroPlusZero() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("0+0").applyNumericAffinity();
		assertEquals(value.asString(), "0+0");
	}

	@Test
	void plusSeven() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("+7").applyNumericAffinity();
		assertEquals(value.asInt(), 7);
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
		SQLite3Constant value = SQLite3TextConstant.createTextConstant(".5").applyNumericAffinity();
		assertEquals(value.asDouble(), 0.5);
	}

	@Test
	void plusMinusZero() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("+-0").applyNumericAffinity();
		assertEquals(value.asString(), "+-0");
	}

	@Test
	void plusPlusZero() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("++0").applyNumericAffinity();
		assertEquals(value.asString(), "++0");
	}

	// other tests

	@Test
	void testZeroSeven() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("07").applyNumericAffinity();
		assertEquals(value.asInt(), 7);
	}

	@Test
	void testSpace() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("8 ").applyNumericAffinity();
		assertEquals(value.asInt(), 8);
	}

	@Test
	void looksLikeReal1() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant(".5d").applyNumericAffinity();
		assertEquals(value.asString(), ".5d");
	}

	@Test
	void looksLikeReal2() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("0.5f").applyNumericAffinity();
		assertEquals(value.asString(), "0.5f");
	}

	@Test
	void looksLikeReal3() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("0.5D").applyNumericAffinity();
		assertEquals(value.asString(), "0.5D");
	}

	@Test
	void looksLikeReal4() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("-03").applyNumericAffinity();
		assertEquals(value.asInt(), -3);
	}

}
