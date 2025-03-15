package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
    public void testGetBigInteger() {
        Randomly r = new Randomly();
        BigInteger min = BigInteger.valueOf(-100);
        BigInteger max = BigInteger.valueOf(100);

        BigInteger result = r.getBigInteger(BigInteger.TEN, BigInteger.TEN);
        assertEquals(BigInteger.TEN, result);

        boolean foundPositive = false;
        boolean foundNegative = false;
        boolean foundZero = false;
        
        int runs = 1000;
        for (int i = 0; i < runs; i++) {
            try {
                BigInteger val = r.getBigInteger(min, max);
                assertTrue(val.compareTo(min) >= 0 && val.compareTo(max) <= 0);
                
                if (val.compareTo(BigInteger.ZERO) > 0) {
                    foundPositive = true;
                } else if (val.compareTo(BigInteger.ZERO) < 0) {
                    foundNegative = true;
                } else {
                    foundZero = true;
                }
            } catch (IgnoreMeException e) {
            }
        }
        
        assertTrue(foundPositive && foundNegative && foundZero);
    }
    
    @Test
    public void testGetRandomBigDecimal() {
        Randomly r = new Randomly();
        boolean foundPositive = false;
        boolean foundLessThanHalf = false;
        boolean foundGreaterThanHalf = false;
        
        for (int i = 0; i < 1000; i++) {
            BigDecimal value = r.getRandomBigDecimal();
            assertTrue(value.compareTo(BigDecimal.ZERO) >= 0 && value.compareTo(BigDecimal.ONE) <= 0);
            
            if (value.compareTo(BigDecimal.ZERO) > 0) {
                foundPositive = true;
            }
            
            if (value.compareTo(new BigDecimal("0.5")) < 0) {
                foundLessThanHalf = true;
            } else {
                foundGreaterThanHalf = true;
            }
        }
        
        assertTrue(foundPositive);
        assertTrue(foundLessThanHalf);
        assertTrue(foundGreaterThanHalf);
    }
    
    @Test
    public void testGetChar() {
        Randomly r = new Randomly();
        boolean foundAlphabetic = false;
        boolean foundDigit = false;
        boolean foundSpecial = false;
        
        for (int i = 0; i < 1000; i++) {
            String charString = r.getChar();
            assertEquals(1, charString.length());
            
            char c = charString.charAt(0);
            if (Character.isAlphabetic(c)) {
                foundAlphabetic = true;
            } else if (Character.isDigit(c)) {
                foundDigit = true;
            } else {
                foundSpecial = true;
            }
        }
        
        assertTrue(foundAlphabetic);
        assertTrue(foundDigit);
        assertTrue(foundSpecial);
    }
    
    @Test
    public void testGetAlphabeticChar() {
        Randomly r = new Randomly();
        boolean foundUppercase = false;
        boolean foundLowercase = false;
        
        for (int i = 0; i < 1000; i++) {
            String charString = r.getAlphabeticChar();
            assertEquals(1, charString.length());
            
            char c = charString.charAt(0);
            assertTrue(Character.isAlphabetic(c));
            
            if (Character.isUpperCase(c)) {
                foundUppercase = true;
            } else if (Character.isLowerCase(c)) {
                foundLowercase = true;
            }
        }
        
        assertTrue(foundUppercase);
        assertTrue(foundLowercase);
    }
    
    @Test
    public void testGetPositiveIntegerNotNull() {
        Randomly r = new Randomly();
        boolean foundSmall = false;
        boolean foundLarge = false;
        
        for (int i = 0; i < 1000; i++) {
            long value = r.getPositiveIntegerNotNull();
            assertTrue(value > 0);
            
            if (value <= 100) {
                foundSmall = true;
            }
            if (value > 1000000) {
                foundLarge = true;
            }
        }
        
        assertTrue(foundSmall);
        assertTrue(foundLarge);
    }
    
    @Test
    public void testNonEmptySubsetLeast() {
        List<Integer> options = Arrays.asList(1, 2, 3, 4, 5);
        int minSize = 2;
        
        boolean foundExactMin = false;
        boolean foundLarger = false;
        boolean foundOriginalSet = false;
        
        for (int i = 0; i < 1000; i++) {
            List<Integer> subset = Randomly.nonEmptySubsetLeast(options, minSize);
            
            assertTrue(subset.size() >= minSize);
            assertTrue(options.containsAll(subset));
            
            if (subset.size() == minSize) {
                foundExactMin = true;
            } else if (subset.size() > minSize && subset.size() < options.size()) {
                foundLarger = true;
            } else if (subset.size() == options.size()) {
                foundOriginalSet = true;
            }
        }
        
        assertTrue(foundExactMin);
        assertTrue(foundLarger);
        assertTrue(foundOriginalSet);
    }
@Test
    public void testNonEmptySubsetVariants() {

        Integer[] options = { 1, 2, 3, 4, 5 };
        
        boolean foundSingleElement = false;
        boolean foundMultipleElements = false;
        boolean foundAllElements = false;
        
        for (int i = 0; i < 1000; i++) {
            List<Integer> subset = Randomly.nonEmptySubset(options);

            assertFalse(subset.isEmpty());

            for (Integer element : subset) {
                boolean found = false;
                for (Integer option : options) {
                    if (option.equals(element)) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found);
            }
            
            if (subset.size() == 1) {
                foundSingleElement = true;
            } else if (subset.size() > 1 && subset.size() < options.length) {
                foundMultipleElements = true;
            } else if (subset.size() == options.length) {
                foundAllElements = true;
            }
        }
        
        assertTrue(foundSingleElement);
        assertTrue(foundMultipleElements);
        assertTrue(foundAllElements);
    }
    
    @Test
    public void testNonEmptySubsetList() {
        List<String> options = Arrays.asList("a", "b", "c", "d");
        
        boolean foundSingleElement = false;
        boolean foundMultipleElements = false;
        boolean foundAllElements = false;
        
        for (int i = 0; i < 1000; i++) {
            List<String> subset = Randomly.nonEmptySubset(options);

            assertFalse(subset.isEmpty());

            assertTrue(options.containsAll(subset));
            
            if (subset.size() == 1) {
                foundSingleElement = true;
            } else if (subset.size() > 1 && subset.size() < options.size()) {
                foundMultipleElements = true;
            } else if (subset.size() == options.size()) {
                foundAllElements = true;
            }
        }
        
        assertTrue(foundSingleElement);
        assertTrue(foundMultipleElements);
        assertTrue(foundAllElements);
    }
    
    @Test
    public void testNonEmptySubsetWithSize() {
        List<Integer> options = Arrays.asList(1, 2, 3, 4, 5);
        int specifiedSize = 3;
        
        for (int i = 0; i < 100; i++) {
            List<Integer> subset = Randomly.nonEmptySubset(options, specifiedSize);

            assertEquals(specifiedSize, subset.size());

            assertTrue(options.containsAll(subset));

            assertEquals(subset.size(), subset.stream().distinct().count());
        }

        assertThrows(AssertionError.class, () -> Randomly.nonEmptySubset(options, options.size() + 1));
    }
    
    @Test
    public void testNonEmptySubsetPotentialDuplicates() {
        List<String> options = Arrays.asList("a", "b", "c");
        
        boolean foundSingleElement = false;
        boolean foundMultipleElements = false;
        boolean foundDuplicates = false;
        
        for (int i = 0; i < 1000; i++) {
            List<String> result = Randomly.nonEmptySubsetPotentialDuplicates(options);

            assertFalse(result.isEmpty());

            for (String element : result) {
                assertTrue(options.contains(element));
            }
            
            if (result.size() == 1) {
                foundSingleElement = true;
            } else {
                foundMultipleElements = true;
            }

            if (result.size() != result.stream().distinct().count()) {
                foundDuplicates = true;
            }
        }
        
        assertTrue(foundSingleElement);
        assertTrue(foundMultipleElements);
        assertTrue(foundDuplicates);
    }
    
    @Test
    public void testGetUncachedDouble() {
        boolean foundLessThanHalf = false;
        boolean foundGreaterThanHalf = false;
        
        for (int i = 0; i < 1000; i++) {
            double value = Randomly.getUncachedDouble();

            assertTrue(value >= 0.0 && value <= 1.0);
            
            if (value < 0.5) {
                foundLessThanHalf = true;
            } else {
                foundGreaterThanHalf = true;
            }
        }
        
        assertTrue(foundLessThanHalf);
        assertTrue(foundGreaterThanHalf);
    }
    
    @Test
    public void testGetPositiveOrZeroNonCachedInteger() {
        boolean foundZero = false;
        boolean foundSmall = false;
        boolean foundLarge = false;
        
        for (int i = 0; i < 1000; i++) {
            long value = Randomly.getPositiveOrZeroNonCachedInteger();

            assertTrue(value >= 0);
            
            if (value == 0) {
                foundZero = true;
            } else if (value < 100) {
                foundSmall = true;
            } else if (value > 1000000) {
                foundLarge = true;
            }
        }
        
        assertTrue(foundZero || foundSmall || foundLarge);
    }
    
    @Test
    public void testGetPositiveIntegerInt() {
        Randomly r = new Randomly();
        boolean foundZero = false;
        boolean foundSmall = false;
        boolean foundLarge = false;
        boolean foundMaxValue = false;
        
        for (int i = 0; i < 1000; i++) {
            int value = r.getPositiveIntegerInt();

            assertTrue(value >= 0);

            if (value == 0) {
                foundZero = true;
            } else if (value < 100) {
                foundSmall = true;
            } else if (value > 1000000) {
                foundLarge = true;
            } else if (value == Integer.MAX_VALUE) {
                foundMaxValue = true;
            }
        }

        assertTrue(foundZero || foundSmall || foundLarge || foundMaxValue);
    }
}
