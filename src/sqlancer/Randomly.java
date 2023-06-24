package sqlancer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

public final class Randomly {

    private static StringGenerationStrategy stringGenerationStrategy = StringGenerationStrategy.SOPHISTICATED;
    private static int maxStringLength = 10;
    private static boolean useCaching = true;
    private static int cacheSize = 100;

    private final List<Long> cachedLongs = new ArrayList<>();
    private final List<Integer> cachedIntegers = new ArrayList<>();
    private final List<String> cachedStrings = new ArrayList<>();
    private final List<Double> cachedDoubles = new ArrayList<>();
    private final List<byte[]> cachedBytes = new ArrayList<>();
    private Supplier<String> provider;

    private static final ThreadLocal<Random> THREAD_RANDOM = new ThreadLocal<>();
    private long seed;

    private void addToCache(long val) {
        if (useCaching && cachedLongs.size() < cacheSize && !cachedLongs.contains(val)) {
            cachedLongs.add(val);
        }
    }

    private void addToCache(int val) {
        if (useCaching && cachedIntegers.size() < cacheSize && !cachedIntegers.contains(val)) {
            cachedIntegers.add(val);
        }
    }

    private void addToCache(double val) {
        if (useCaching && cachedDoubles.size() < cacheSize && !cachedDoubles.contains(val)) {
            cachedDoubles.add(val);
        }
    }

    private void addToCache(String val) {
        if (useCaching && cachedStrings.size() < cacheSize && !cachedStrings.contains(val)) {
            cachedStrings.add(val);
        }
    }

    private Long getFromLongCache() {
        if (!useCaching || cachedLongs.isEmpty()) {
            return null;
        } else {
            return Randomly.fromList(cachedLongs);
        }
    }

    private Integer getFromIntegerCache() {
        if (!useCaching || cachedIntegers.isEmpty()) {
            return null;
        } else {
            return Randomly.fromList(cachedIntegers);
        }
    }

    private Double getFromDoubleCache() {
        if (!useCaching) {
            return null;
        }
        if (Randomly.getBoolean() && !cachedLongs.isEmpty()) {
            return (double) Randomly.fromList(cachedLongs);
        } else if (!cachedDoubles.isEmpty()) {
            return Randomly.fromList(cachedDoubles);
        } else {
            return null;
        }
    }

    private String getFromStringCache() {
        if (!useCaching) {
            return null;
        }
        if (Randomly.getBoolean() && !cachedLongs.isEmpty()) {
            return String.valueOf(Randomly.fromList(cachedLongs));
        } else if (Randomly.getBoolean() && !cachedDoubles.isEmpty()) {
            return String.valueOf(Randomly.fromList(cachedDoubles));
        } else if (Randomly.getBoolean() && !cachedBytes.isEmpty()
                && stringGenerationStrategy == StringGenerationStrategy.SOPHISTICATED) {
            return new String(Randomly.fromList(cachedBytes));
        } else if (!cachedStrings.isEmpty()) {
            String randomString = Randomly.fromList(cachedStrings);
            if (Randomly.getBoolean()) {
                return randomString;
            } else {
                return stringGenerationStrategy.transformCachedString(this, randomString);
            }
        } else {
            return null;
        }
    }

    private static boolean cacheProbability() {
        return useCaching && getNextLong(0, 3) == 1;
    }

    // CACHING END

    public static <T> T fromList(List<T> list) {
        return list.get((int) getNextLong(0, list.size()));
    }

    @SafeVarargs
    public static <T> T fromOptions(T... options) {
        return options[getNextInt(0, options.length)];
    }

    @SafeVarargs
    public static <T> List<T> nonEmptySubset(T... options) {
        int nr = 1 + getNextInt(0, options.length);
        return extractNrRandomColumns(Arrays.asList(options), nr);
    }

    public static <T> List<T> nonEmptySubset(List<T> columns) {
        int nr = 1 + getNextInt(0, columns.size());
        return nonEmptySubset(columns, nr);
    }

    public static <T> List<T> nonEmptySubset(List<T> columns, int nr) {
        if (nr > columns.size()) {
            throw new AssertionError(columns + " " + nr);
        }
        return extractNrRandomColumns(columns, nr);
    }

    public static <T> List<T> nonEmptySubsetPotentialDuplicates(List<T> columns) {
        List<T> arr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            arr.add(Randomly.fromList(columns));
        }
        return arr;
    }

    public static <T> List<T> subset(List<T> columns) {
        int nr = getNextInt(0, columns.size() + 1);
        return extractNrRandomColumns(columns, nr);
    }

    public static <T> List<T> subset(int nr, @SuppressWarnings("unchecked") T... values) {
        List<T> list = new ArrayList<>();
        Collections.addAll(list, values);
        return extractNrRandomColumns(list, nr);
    }

    public static <T> List<T> subset(@SuppressWarnings("unchecked") T... values) {
        List<T> list = new ArrayList<>(Arrays.asList(values));
        return subset(list);
    }

    public static <T> List<T> extractNrRandomColumns(List<T> columns, int nr) {
        assert nr >= 0;
        List<T> selectedColumns = new ArrayList<>();
        List<T> remainingColumns = new ArrayList<>(columns);
        for (int i = 0; i < nr; i++) {
            selectedColumns.add(remainingColumns.remove(getNextInt(0, remainingColumns.size())));
        }
        return selectedColumns;
    }

    public static int smallNumber() {
        // no need to cache for small numbers
        return (int) (Math.abs(getThreadRandom().get().nextGaussian())) * 2;
    }

    public static boolean getBoolean() {
        return getThreadRandom().get().nextBoolean();
    }

    public static double getPercentage() {
        return getThreadRandom().get().nextDouble();
    }

    private static ThreadLocal<Random> getThreadRandom() {
        if (THREAD_RANDOM.get() == null) {
            // a static method has been called, before Randomly was instantiated
            THREAD_RANDOM.set(new Random());
        }
        return THREAD_RANDOM;
    }

    public long getInteger() {
        if (smallBiasProbability()) {
            return Randomly.fromOptions(-1L, Long.MAX_VALUE, Long.MIN_VALUE, 1L, 0L);
        } else {
            if (cacheProbability()) {
                Long l = getFromLongCache();
                if (l != null) {
                    return l;
                }
            }
            long nextLong = getThreadRandom().get().nextInt();
            addToCache(nextLong);
            return nextLong;
        }
    }

    public enum StringGenerationStrategy {

        NUMERIC {
            @Override
            public String getString(Randomly r) {
                return getStringOfAlphabet(r, NUMERIC_ALPHABET);
            }

        },
        ALPHANUMERIC {
            @Override
            public String getString(Randomly r) {
                return getStringOfAlphabet(r, ALPHANUMERIC_ALPHABET);

            }

        },
        ALPHANUMERIC_SPECIALCHAR {
            @Override
            public String getString(Randomly r) {
                return getStringOfAlphabet(r, ALPHANUMERIC_SPECIALCHAR_ALPHABET);

            }

        },
        SOPHISTICATED {

            private static final String ALPHABET = ALPHANUMERIC_SPECIALCHAR_ALPHABET;

            @Override
            public String getString(Randomly r) {
                if (smallBiasProbability()) {
                    return Randomly.fromOptions("TRUE", "FALSE", "0.0", "-0.0", "1e500", "-1e500");
                }
                if (cacheProbability()) {
                    String s = r.getFromStringCache();
                    if (s != null) {
                        return s;
                    }
                }

                int n = ALPHABET.length();

                StringBuilder sb = new StringBuilder();

                int chars = getStringLength(r);
                for (int i = 0; i < chars; i++) {
                    if (Randomly.getBooleanWithRatherLowProbability()) {
                        char val = (char) r.getInteger();
                        if (val != 0) {
                            sb.append(val);
                        }
                    } else {
                        sb.append(ALPHABET.charAt(getNextInt(0, n)));
                    }
                }
                while (Randomly.getBooleanWithSmallProbability()) {
                    String[][] pairs = { { "{", "}" }, { "[", "]" }, { "(", ")" } };
                    int idx = (int) Randomly.getNotCachedInteger(0, pairs.length);
                    int left = (int) Randomly.getNotCachedInteger(0, sb.length() + 1);
                    sb.insert(left, pairs[idx][0]);
                    int right = (int) Randomly.getNotCachedInteger(left + 1, sb.length() + 1);
                    sb.insert(right, pairs[idx][1]);
                }
                if (r.provider != null) {
                    while (Randomly.getBooleanWithSmallProbability()) {
                        if (sb.length() == 0) {
                            sb.append(r.provider.get());
                        } else {
                            sb.insert((int) Randomly.getNotCachedInteger(0, sb.length()), r.provider.get());
                        }
                    }
                }

                String s = sb.toString();

                r.addToCache(s);
                return s;
            }

            public String transformCachedString(Randomly r, String randomString) {
                if (Randomly.getBoolean()) {
                    return randomString.toLowerCase();
                } else if (Randomly.getBoolean()) {
                    return randomString.toUpperCase();
                } else {
                    char[] chars = randomString.toCharArray();
                    if (chars.length != 0) {
                        for (int i = 0; i < Randomly.smallNumber(); i++) {
                            chars[r.getInteger(0, chars.length)] = ALPHABET.charAt(r.getInteger(0, ALPHABET.length()));
                        }
                    }
                    return new String(chars);
                }
            }

        };

        private static final String ALPHANUMERIC_SPECIALCHAR_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#<>/.,~-+'*()[]{} ^*?%_\t\n\r|&\\";
        private static final String ALPHANUMERIC_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        private static final String NUMERIC_ALPHABET = "0123456789";

        private static int getStringLength(Randomly r) {
            int chars;
            if (Randomly.getBoolean()) {
                chars = Randomly.smallNumber();
            } else {
                chars = r.getInteger(0, maxStringLength);
            }
            return chars;
        }

        private static String getStringOfAlphabet(Randomly r, String alphabet) {
            int chars = getStringLength(r);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < chars; i++) {
                sb.append(alphabet.charAt(getNextInt(0, alphabet.length())));
            }
            return sb.toString();
        }

        public abstract String getString(Randomly r);

        public String transformCachedString(Randomly r, String s) {
            return s;
        }

    }

    public String getString() {
        return stringGenerationStrategy.getString(this);
    }

    public byte[] getBytes() {
        int size = Randomly.smallNumber();
        byte[] arr = new byte[size];
        getThreadRandom().get().nextBytes(arr);
        return arr;
    }

    public long getNonZeroInteger() {
        long value;
        if (smallBiasProbability()) {
            return Randomly.fromOptions(-1L, Long.MAX_VALUE, Long.MIN_VALUE, 1L);
        }
        if (cacheProbability()) {
            Long l = getFromLongCache();
            if (l != null && l != 0) {
                return l;
            }
        }
        do {
            value = getInteger();
        } while (value == 0);
        addToCache(value);
        return value;
    }

    public long getPositiveInteger() {
        if (cacheProbability()) {
            Long value = getFromLongCache();
            if (value != null && value >= 0) {
                return value;
            }
        }
        long value;
        if (smallBiasProbability()) {
            value = Randomly.fromOptions(0L, Long.MAX_VALUE, 1L);
        } else {
            value = getNextLong(0, Long.MAX_VALUE);
        }
        addToCache(value);
        assert value >= 0;
        return value;
    }

    public int getPositiveIntegerInt() {
        if (cacheProbability()) {
            Integer value = getFromIntegerCache();
            if (value != null && value >= 0) {
                return value;
            }
        }
        int value;
        if (smallBiasProbability()) {
            value = Randomly.fromOptions(0, Integer.MAX_VALUE, 1);
        } else {
            value = getNextInt(0, Integer.MAX_VALUE);
        }
        addToCache(value);
        assert value >= 0;
        return value;
    }

    public double getFiniteDouble() {
        while (true) {
            double val = getDouble();
            if (Double.isFinite(val)) {
                return val;
            }
        }
    }

    public double getDouble() {
        if (smallBiasProbability()) {
            return Randomly.fromOptions(0.0, -0.0, Double.MAX_VALUE, -Double.MAX_VALUE, Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY);
        } else if (cacheProbability()) {
            Double d = getFromDoubleCache();
            if (d != null) {
                return d;
            }
        }
        double value = getThreadRandom().get().nextDouble();
        addToCache(value);
        return value;
    }

    private static boolean smallBiasProbability() {
        return getThreadRandom().get().nextInt(100) == 1;
    }

    public static boolean getBooleanWithRatherLowProbability() {
        return getThreadRandom().get().nextInt(10) == 1;
    }

    public static boolean getBooleanWithSmallProbability() {
        return smallBiasProbability();
    }

    public int getInteger(int left, int right) {
        if (left == right) {
            return left;
        }
        return (int) getLong(left, right);
    }

    // TODO redundant?
    public long getLong(long left, long right) {
        if (left == right) {
            return left;
        }
        return getNextLong(left, right);
    }

    public BigInteger getBigInteger(BigInteger left, BigInteger right) {
        if (left.equals(right)) {
            return left;
        }
        if (right.subtract(left).abs().compareTo(new BigInteger("10000")) <= 0) {
            return left.add(right).mod(new BigInteger("2"));
        }
        while (true) {
            BigInteger result = new BigInteger(63, new Random());
            if (result.compareTo(left) >= 0 && result.compareTo(right) <= 0) {
                return result;
            }
        }
    }

    public BigDecimal getRandomBigDecimal() {
        return BigDecimal.valueOf(getThreadRandom().get().nextDouble());
    }

    public long getPositiveIntegerNotNull() {
        while (true) {
            long val = getPositiveInteger();
            if (val != 0) {
                return val;
            }
        }
    }

    public static long getNonCachedInteger() {
        return getThreadRandom().get().nextLong();
    }

    public static long getPositiveOrZeroNonCachedInteger() {
        return getNextLong(0, Long.MAX_VALUE);
    }

    public static long getNotCachedInteger(int lower, int upper) {
        return getNextLong(lower, upper);
    }

    public Randomly(Supplier<String> provider) {
        this.provider = provider;
    }

    public Randomly() {
        THREAD_RANDOM.set(new Random());
    }

    public Randomly(long seed) {
        this.seed = seed;
        THREAD_RANDOM.set(new Random(seed));
    }

    public static double getUncachedDouble() {
        return getThreadRandom().get().nextDouble();
    }

    public String getChar() {
        while (true) {
            String s = getString();
            if (!s.isEmpty()) {
                return s.substring(0, 1);
            }
        }
    }

    public String getAlphabeticChar() {
        while (true) {
            String s = getChar();
            if (Character.isAlphabetic(s.charAt(0))) {
                return s;
            }
        }
    }

    // see https://stackoverflow.com/a/2546158
    // uniformity does not seem to be important for us
    // SQLancer previously used ThreadLocalRandom.current().nextLong(lower, upper)
    private static long getNextLong(long lower, long upper) {
        if (lower > upper) {
            throw new IllegalArgumentException(lower + " " + upper);
        }
        if (lower == upper) {
            return lower;
        }
        return getThreadRandom().get().longs(lower, upper).findFirst().getAsLong();
    }

    private static int getNextInt(int lower, int upper) {
        return (int) getNextLong(lower, upper);
    }

    public long getSeed() {
        return seed;
    }

    public static void initialize(MainOptions options) {
        stringGenerationStrategy = options.getRandomStringGenerationStrategy();
        maxStringLength = options.getMaxStringConstantLength();
        useCaching = options.useConstantCaching();
        cacheSize = options.getConstantCacheSize();
    }

}
