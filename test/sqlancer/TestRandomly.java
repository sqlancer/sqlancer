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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class TestRandomly {

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
        int i = 0;
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
        } while (!encounteredEmptySubset || !encounteredOriginalSet || !encounteredStrictSubsetNonEmpty
                || i++ < NR_MIN_RUNS);

        assertTrue(encounteredEmptySubset, "Empty subset was not encountered");
        assertTrue(encounteredOriginalSet, "Original set was not encountered");
        assertTrue(encounteredStrictSubsetNonEmpty, "Strict subset was not encountered");

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
        assertTrue(encounteredInteger, "Integer was not encountered");
        assertTrue(encounteredAscii, "Ascii was not encountered");
        assertTrue(encounteredNonAscii, "Non ascii was not encountered");
        assertTrue(encounteredSpace, "Space was not encountered");
    }

    @Test // TODO: also generate and check for NaN
    public void testDouble() {
        Randomly r = new Randomly();
        boolean encounteredZero = false;
        boolean encounteredPositive = false;
        boolean encounteredNegative = false;
        boolean encounteredInfinity = false;
        int i = 0;
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
        } while (!encounteredZero || !encounteredPositive || !encounteredNegative || !encounteredInfinity
                || i++ < NR_MIN_RUNS);
        assertTrue(encounteredZero, "Zero was not encountered");
        assertTrue(encounteredPositive, "Positive was not encountered");
        assertTrue(encounteredNegative, "Negative was not encountered");
        assertTrue(encounteredInfinity, "Infinity was not encountered");
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
        assertTrue(encounteredPositive, "Positive integer was not encountered");
        assertTrue(encounteredNegative, "Negative integer was not encountered");
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
        assertTrue(encounteredZero, "Zero was not encountered");
        assertTrue(encounteredMaxValue, "Max value was not encountered");
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
        assertTrue(encounteredAllZeroes, "All zeroes were not encountered");
        assertTrue(encounteredMax, "Max value was not encountered");
        assertTrue(encounteredZeroLength, "Zero length was not encountered");
    }

    @Test
    public void testNonCachedInteger() {
        assertEquals(0, Randomly.getNotCachedInteger(0, 1));
        assertEquals(0, Randomly.getNotCachedInteger(0, 0));
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

    @Test
    public void testLong2() {
        Randomly r = new Randomly();
        for (int i = 0; i < NR_MIN_RUNS; i++) {
            long val = r.getLong(-1, Long.MAX_VALUE);
            assertTrue(val >= -1);
            assertTrue(val < Long.MAX_VALUE);
        }
    }

    @Test // check that when given a seed, each thread computes a consistent result
    public void testSeed() {
        int seed = 123;
        Randomly r = new Randomly(seed);
        List<String> values = getRandomValueList(r);
        List<String> nonSeedList = getRandomValueList(new Randomly());
        List<List<String>> otherThreadResults = new ArrayList<>();
        assertNotEquals(values, nonSeedList);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 1000; i++) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    otherThreadResults.add(getRandomValueList(new Randomly(seed)));
                }
            });
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        for (List<String> otherThreadResult : otherThreadResults) {
            assertEquals(values, otherThreadResult);
        }
    }

    private List<String> getRandomValueList(Randomly r) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add(String.valueOf(r.getDouble()));
            values.add(String.valueOf(r.getInteger()));
            values.add(String.valueOf(r.getString()));
            values.add(String.valueOf(Randomly.getBoolean()));
        }
        return values;
    }

    @Test
    public void testGetPercentage() {
        for (int i = 0; i < NR_MIN_RUNS; i++) {
            double percentage = Randomly.getPercentage();
            assertTrue(percentage >= 0.0);
            assertTrue(percentage <= 1.0);
        }
    }

    @Test
    public void testGetChar() {
        Randomly r = new Randomly();
        boolean encounteredAlphabetic = false;
        boolean encounteredNumeric = false;
        boolean encounteredSpecial = false;
        int i = 0;
        do {
            String c = r.getChar();
            assertEquals(1, c.length());
            if (Character.isAlphabetic(c.charAt(0))) {
                encounteredAlphabetic = true;
            } else if (Character.isDigit(c.charAt(0))) {
                encounteredNumeric = true;
            } else {
                encounteredSpecial = true;
            }
        } while (!encounteredAlphabetic || !encounteredNumeric || !encounteredSpecial || i++ < NR_MIN_RUNS);
        assertTrue(encounteredAlphabetic, "Never encounter an alphabetic character.");
        assertTrue(encounteredNumeric, "Never encounter a numeric character.");
        assertTrue(encounteredSpecial, "Never encounter a special character.");
    }

    @Test
    public void testGetAlphabeticChar() {
        Randomly r = new Randomly();
        for (int i = 0; i < NR_MIN_RUNS; i++) {
            String c = r.getAlphabeticChar();
            assertEquals(1, c.length());
            assertTrue(Character.isAlphabetic(c.charAt(0)));
        }
    }

    @Test
    public void testGetBooleanWithSmallProbability() {
        int trueCount = 0;
        int totalRuns = NR_MIN_RUNS;

        for (int i = 0; i < totalRuns; i++) {
            if (Randomly.getBooleanWithSmallProbability()) {
                trueCount++;
            }
        }

        double trueRatio = (double) trueCount / totalRuns;
        assertTrue(trueRatio > 0.005);
        assertTrue(trueRatio < 0.015);

    }

}
