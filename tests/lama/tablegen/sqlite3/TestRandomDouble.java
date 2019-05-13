package lama.tablegen.sqlite3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lama.Randomly;

class TestRandomDouble {

	// test <

	private static final int NR_RANDOM_TESTS = 100000;
	Random random = new Random(0);
	Randomly r = new Randomly();

	@Test
	void testLessMinValue() {
		assertEquals(r.smallerDouble(-Double.MAX_VALUE), Double.NEGATIVE_INFINITY);
	}

	@Test
	void testLessNegativeInfinity() {
		Assertions.assertThrows(Exception.class, () -> r.smallerDouble(Double.NEGATIVE_INFINITY));
	}

	@Test
	void testLessRandom() {
		for (int i = 0; i < 100000; i++) {
			double rVal = random.nextDouble();
			double less = r.smallerDouble(rVal);
			assertTrue(less < rVal, rVal + " " + less);
		}
	}

	// test >

	@Test
	void testLessMaxValue() {
		assertEquals(r.greaterDouble(Double.MAX_VALUE), Double.POSITIVE_INFINITY);
	}

	@Test
	void testGreaterPositiveInfinity() {
		Assertions.assertThrows(Exception.class, () -> r.greaterDouble(Double.POSITIVE_INFINITY));
	}

	@Test
	void testGreaterRandom() {
		for (int i = 0; i < NR_RANDOM_TESTS; i++) {
			double val = random.nextDouble();
			double less = r.greaterDouble(val);
			assertTrue(less > val, val + " " + less);
		}
	}

}
