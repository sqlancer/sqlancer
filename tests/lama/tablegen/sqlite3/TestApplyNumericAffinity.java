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
	void zeroPoint() {
		SQLite3Constant pointZero = SQLite3TextConstant.createTextConstant("0.").applyNumericAffinity();
		assertEquals(pointZero.asInt(), 0);
	}

	@Test
	void zeroE() {
		SQLite3Constant pointZero = SQLite3TextConstant.createTextConstant("0e6").applyNumericAffinity();
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
	void testENotation() {
		expectReal("-1.248781264E9", -1.248781264E9);
	}

	@Test
	void testDigit() {
		expectInt("5", 5);
	}

	@Test
	void testLargeNumber() {
		expectInt("2054756498", 2054756498);
	}

	@Test
	void testReal() {
		expectReal("0.8170119915169454", 0.8170119915169454);
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

	@Test
	void emptyString() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("").applyNumericAffinity();
		assertEquals(value.asString(), "");
	}

	@Test
	void e() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("e").applyNumericAffinity();
		assertEquals(value.asString(), "e");
	}

	@Test
	void sevenE() {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant("7e").applyNumericAffinity();
		assertEquals(value.asString(), "7e");
	}

	@Test
	void testLeadingZero() {
		expectInt("08.", 8);
	}

	@Test
	void testStrangeExample1() {
		expectInt("+005.000", 5);
	}

	@Test
	void testStrangeExample2() {
		expectReal("-005.500", -5.5);
	}

	public void expectInt(String text, int val) {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant(text).applyNumericAffinity();
		assertEquals(value.asInt(), val);
	}

	public void expectReal(String text, double val) {
		SQLite3Constant value = SQLite3TextConstant.createTextConstant(text).applyNumericAffinity();
		assertEquals(value.asDouble(), val);
	}

//	@Test
//	void testApplyBinaryAffinity() {
//		SQLite3Constant value = SQLite3TextConstant.createBinaryConstant(new byte[] {(byte) 0x81}).applyTextAffinity();
//		SQLite3Constant value2 = SQLite3TextConstant.createBinaryConstant(new byte[] {0x06, 0x13, 0x49}).applyTextAffinity();
//	 List<BinaryOperator> result = value.compare(value, true);
//		assertEquals(value.asInt(), -3);
//	}

}
