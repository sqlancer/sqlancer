package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
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
    public void testNonEmptySubsetVarArgs() {
        Integer[] options = { 1, 2, 3, 4, 5 };
        List<Integer> subset = Randomly.nonEmptySubset(options);
        // subset =null;
        assertFalse(subset.isEmpty(), "method nonEmptySubset (VarArgs) should return a subset not empty");
        for (Integer i : subset) {
            assertTrue(Arrays.asList(options).contains(i));
        }
    }

    @Test
    public void testNonEmptySubsetList() {
        List<String> options = Arrays.asList("a", "b", "c", "d");
        List<String> subset = Randomly.nonEmptySubset(options);
        assertFalse(subset.isEmpty(), "nonEmptySubset (list) should return a subset not empty");
        for (String s : subset) {
            assertTrue(options.contains(s));
        }
    }

    @Test
    public void testNonEmptySubsetLeast() {
        List<String> options = Arrays.asList("a", "b", "c", "d", "e", "f");
        int min = 2;
        List<String> subset = Randomly.nonEmptySubsetLeast(options, min);
        assertTrue(subset.size() >= min, "nonEmptySubsetLeast should return at least " + min + " elements");
        for (String s : subset) {
            assertTrue(options.contains(s));
        }
    }

    @Test
    public void testSmallNumber() {
        int num = Randomly.smallNumber();
        assertTrue(num >= 0, "smallNumber should be non-negtive");
        boolean foundPositive = false;
        for (int i = 0; i < 80; i++) {
            if (Randomly.smallNumber() > 0) {
                foundPositive = true;
                break;
            }
        }
        assertTrue(foundPositive, "smallNumber should  return a positive number");
    }

    @Test
    public void testGetBigInteger() {
        BigInteger left = BigInteger.valueOf(10);
        BigInteger right = BigInteger.valueOf(100);
        Randomly r = new Randomly();

        assertEquals(left, r.getBigInteger(left, left));
        BigInteger bi = r.getBigInteger(left, right);

        assertTrue(bi.compareTo(left) >= 0 && bi.compareTo(right) <= 0,
                "BigInteger should be between left and right (inclusive right and left)");
    }

    @Test
    public void testGetRandomBigDecimal() {
        Randomly r = new Randomly();
        BigDecimal bd = r.getRandomBigDecimal();

        assertTrue(bd.compareTo(BigDecimal.ZERO) >= 0 && bd.compareTo(BigDecimal.ONE) < 0,
                "RandomBigDecimal should be between 0.0 (inclusive it) and 1.0 (exclusive it)");
    }

    @Test
    public void testGetPositiveIntegerNotNull() {
        Randomly r = new Randomly();
        for (int i = 0; i < 100; i++) {
            long val = r.getPositiveIntegerNotNull();

            assertNotEquals(0, val, "val should not return zero");
            assertTrue(val >= 0, "val should be non-negative");
        }
    }

    @Test
    public void testGetNonCachedIntegerStatic() {
        long val = Randomly.getNonCachedInteger();

        assertNotNull(val);
    }

    @Test
    public void testGetAlphabeticChar() {
        Randomly r = new Randomly();
        for (int i = 0; i < 1; i++) {
            String c = r.getAlphabeticChar();
            assertNotNull(c, "getAlphabeticChar should not return null");
            assertEquals(1, c.length(), "getAlphabeticChar should return a single character");
            assertTrue(Character.isAlphabetic(c.charAt(0)), "getAlphabeticChar should return an alphabetic character");
        }
    }

}