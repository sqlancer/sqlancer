package lama.tablegen.sqlite3;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import lama.sqlite3.ast.SQLite3Constant;

public class TestApplyEquals {

	SQLite3Constant positiveInf = SQLite3Constant.createRealConstant(Double.POSITIVE_INFINITY);
	SQLite3Constant negativeInf = SQLite3Constant.createRealConstant(Double.NEGATIVE_INFINITY);
	private SQLite3Constant three = SQLite3Constant.createIntConstant(3);

	@Test // SELECT 1e500 = 3;
	public void testInfInt1() {
		assertEquals(0, positiveInf.applyEquals(three).asInt());
	}

	@Test // SELECT 3 = 1e500;
	public void testInfInt2() {
		assertEquals(0, three.applyEquals(positiveInf).asInt());
	}

	@Test // SELECT 1e500 = 1e500;
	public void testInfInf() {
		assertEquals(1, positiveInf.applyEquals(positiveInf).asInt());
	}

	@Test // SELECT 1e500 = -1e500; -- 0
	public void testInfNegativeInf() {
		assertEquals(0, positiveInf.applyEquals(negativeInf).asInt());
	}
}
