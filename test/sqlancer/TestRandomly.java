package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

public class TestRandomly {

    private static final int NR_CHECKS = 10000;
    private static final int NR_MIN_RUNS = 100000;

    @Test // test that every option is picked
    public void testFromOptions() {
        Integer[] options = { 1, 2, 3 };
        List<Integer> remainingOptions = new ArrayList<>(Arrays.asList(options));
        while (!remainingOptions.isEmpty()) {
            Integer pickedOption = Randomly.fromOptions(options);
            remainingOptions.remove(pickedOption);
        }
        assertTrue(remainingOptions.isEmpty());
    }

    @Test
    public void testSubset() {
        boolean encounteredEmptySubset = false;
        boolean encounteredOriginalSet = false;
        boolean encounteredStrictSubsetNonEmpty = false;
        Integer[] options = { 1, 2, 3 };
        List<Integer> optionList = new ArrayList<>(Arrays.asList(options));
        do {
            List<Integer> subset = Randomly.subset(optionList);
            assertEquals(optionList.size(), 3); // check that the original set hasn't been modified
            assertTrue(optionList.containsAll(subset));
            if (subset.isEmpty()) {
                encounteredEmptySubset = true;
            } else if (subset.size() == optionList.size()) {
                encounteredOriginalSet = true;
            } else {
                encounteredStrictSubsetNonEmpty = true;
            }
        } while (!encounteredEmptySubset || !encounteredOriginalSet || !encounteredStrictSubsetNonEmpty);
    }

    @Test
    public void testString() {
        boolean encounteredInteger = false;
        boolean encounteredAscii = false;
        boolean encounteredNonAscii = false;
        boolean encounteredSpace = false;
        Randomly r = new Randomly();
        int i = 0;
        do {
            String s = r.getString();
            for (Character c : s.toCharArray()) {
                if (Character.isAlphabetic(c)) {
                    encounteredAscii = true;
                } else if (Character.isDigit(c)) {
                    encounteredInteger = true;
                } else if (Character.isSpaceChar(c)) {
                    encounteredSpace = true;
                } else {
                    encounteredNonAscii = true;
                }
            }
        } while (!encounteredInteger || !encounteredAscii || !encounteredNonAscii || !encounteredSpace
                || i++ < NR_MIN_RUNS);
    }

    @Test // TODO: also generate and check for NaN
    public void testDouble() {
        Randomly r = new Randomly();
        boolean encounteredZero = false;
        boolean encounteredPositive = false;
        boolean encounteredNegative = false;
        boolean encounteredInfinity = false;
        do {
            double doubleVal = r.getDouble();
            if (doubleVal == 0) {
                encounteredZero = true;
            } else if (Double.isInfinite(doubleVal)) {
                encounteredInfinity = true;
            } else if (doubleVal > 0) {
                encounteredPositive = true;
            } else if (doubleVal < 0) {
                encounteredNegative = true;
            } else {
                fail(String.valueOf(doubleVal));
            }
        } while (!encounteredZero || !encounteredPositive || !encounteredNegative || !encounteredInfinity);
    }

    @Test
    public void testFiniteDouble() {
        Randomly r = new Randomly();
        for (int i = 0; i < NR_MIN_RUNS; i++) {
            assertFalse(Double.isInfinite(r.getFiniteDouble()));
        }
    }

    @Test
    public void testNonZeroInteger() {
        Randomly r = new Randomly();
        boolean encounteredPositive = false;
        boolean encounteredNegative = false;
        int i = 0;
        do {
            long nonZeroInt = r.getNonZeroInteger();
            assertNotEquals(0, nonZeroInt);
            if (nonZeroInt > 0) {
                encounteredPositive = true;
            } else {
                encounteredNegative = true;
            }
        } while (!encounteredPositive || !encounteredNegative || i++ < NR_MIN_RUNS);
    }

    @Test
    public void testPositiveInteger() {
        Randomly r = new Randomly();
        boolean encounteredZero = false;
        boolean encounteredMaxValue = false;
        int i = 0;
        do {
            long positiveInt = r.getPositiveInteger();
            assertTrue(positiveInt >= 0);
            if (positiveInt == 0) {
                encounteredZero = true;
            } else if (positiveInt == Long.MAX_VALUE) {
                encounteredMaxValue = true;
            }
        } while (!encounteredZero || !encounteredMaxValue || i++ < NR_MIN_RUNS);
    }

    @Test
    public void testBytes() {
        Randomly r = new Randomly();
        boolean encounteredAllZeroes = false;
        boolean encounteredMax = false;
        boolean encounteredZeroLength = false;
        int i = 0;
        do {
            byte[] bytes = r.getBytes();
            if (bytes.length == 0) {
                encounteredZeroLength = true;
            } else if (bytes[0] == 0) {
                encounteredAllZeroes = true;
            } else if (bytes[0] == Byte.MAX_VALUE) {
                encounteredMax = true;
            }
        } while (!encounteredAllZeroes || !encounteredMax || !encounteredZeroLength || i++ < NR_MIN_RUNS);
    }

    @Test
    public void testNonCachedInteger() {
        assertEquals(0, Randomly.getNotCachedInteger(0, 1));
        assertThrows(Exception.class, () -> Randomly.getNotCachedInteger(0, 0));
        assertThrows(Exception.class, () -> Randomly.getNotCachedInteger(5, 0));
    }

    @Test
    public void testInteger() {
        Randomly r = new Randomly();
        // TODO: we should throw an exception instead
        assertEquals(0, r.getInteger(0, 0));
        assertEquals(0, r.getInteger(0, 1));
    }

    @Test
    public void testLong() {
        Randomly r = new Randomly();
        // TODO: we should throw an exception instead
        assertEquals(0, r.getLong(0, 0));
        assertEquals(0, r.getLong(0, 1));
    }

}
