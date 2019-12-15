package lama;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public final class Randomly {

	// CACHING

//	private class Cache<T> {
//		private final List<T> cachedValues = new ArrayList<>();
//		
//		void addToCache(T val) {
//			if (USE_CACHING && cachedValues.size() < CACHE_SIZE && !cachedValues.contains(val)) {
//				cachedValues.add(val);
//			}	
//		}
//	}
//	
//	private final Cache<Long> longCache = new Cache<Long>();
//	private final Cache<String> stringCache = new Cache<String>();
//	private final Cache<Double> doubleCache = new Cache<Double>();
//	private final Cache<Byte[]> bytesCache = new Cache<Byte[]>();

	private static boolean USE_CACHING = true;
	private static final int CACHE_SIZE = 100;

	private final List<Long> cachedLongs = new ArrayList<>();
	private final List<String> cachedStrings = new ArrayList<>();
	private final List<Double> cachedDoubles = new ArrayList<>();
	private final List<byte[]> cachedBytes = new ArrayList<>();
	String alphabet = new String(
			"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzöß!#<>/.,~-+'*()[]{} ^*?%_\t\n\r|&\\");
	private Supplier<String> provider;

	private void addToCache(long val) {
		if (USE_CACHING && cachedLongs.size() < CACHE_SIZE && !cachedLongs.contains(val)) {
			cachedLongs.add(val);
		}
	}

	private void addToCache(double val) {
		if (USE_CACHING && cachedDoubles.size() < CACHE_SIZE && !cachedDoubles.contains(val)) {
			cachedDoubles.add(val);
		}
	}

	private void addToCache(String val) {
		if (USE_CACHING && cachedStrings.size() < CACHE_SIZE && !cachedStrings.contains(val)) {
			cachedStrings.add(val);
		}
	}

	private void addToCache(byte[] val) {
		if (USE_CACHING && cachedBytes.size() < CACHE_SIZE && !cachedBytes.contains(val)) {
			cachedBytes.add(val);
		}
	}

	private byte[] getFromBytesCache() {
		if (!USE_CACHING || cachedBytes.isEmpty()) {
			return null;
		} else {
			byte[] bytes = Randomly.fromList(cachedBytes);
			if (Randomly.getBoolean()) {
				for (int i = 0; i < Randomly.smallNumber(); i++) {
					bytes[getInteger(0, bytes.length)] = (byte) ThreadLocalRandom.current().nextInt();
				}
			}
			return bytes;
		}
	}

	private Long getFromLongCache() {
		if (!USE_CACHING || cachedLongs.isEmpty()) {
			return null;
		} else {
			return Randomly.fromList(cachedLongs);
		}
	}

	private Double getFromDoubleCache() {
		if (!USE_CACHING) {
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
		if (!USE_CACHING) {
			return null;
		}
		if (Randomly.getBoolean() && !cachedLongs.isEmpty()) {
			return String.valueOf(Randomly.fromList(cachedLongs));
		} else if (Randomly.getBoolean() && !cachedDoubles.isEmpty()) {
			return String.valueOf(Randomly.fromList(cachedDoubles));
		} else if (Randomly.getBoolean() && !cachedBytes.isEmpty()) {
			return new String(Randomly.fromList(cachedBytes));
		} else if (!cachedStrings.isEmpty()) {
			String randomString = Randomly.fromList(cachedStrings);
			if (Randomly.getBoolean()) {
				return randomString;
			} else {
				if (Randomly.getBoolean()) {
					return randomString.toLowerCase();
				} else if (Randomly.getBoolean()) {
					return randomString.toUpperCase();
				} else {
					char[] chars = randomString.toCharArray();
					if (chars.length != 0) {
						for (int i = 0; i < Randomly.smallNumber(); i++) {
							chars[getInteger(0, chars.length)] = alphabet.charAt(getInteger(0, alphabet.length()));
						}
					}
					return new String(chars);
				}
			}
		} else {
			return null;
		}
	}

	private static boolean cacheProbability() {
		return USE_CACHING && ThreadLocalRandom.current().nextInt(3) == 1;
	}

	// CACHING END

	public static <T> T fromList(List<T> list) {
		return list.get(ThreadLocalRandom.current().nextInt(list.size()));
	}

	@SafeVarargs
	public static <T> T fromOptions(T... options) {
		return options[ThreadLocalRandom.current().nextInt(options.length)];
	}

	@SafeVarargs
	public static <T> List<T> nonEmptySubset(T... options) {
		int nr = 1 + ThreadLocalRandom.current().nextInt(options.length);
		return extractNrRandomColumns(Arrays.asList(options), nr);
	}

	public static <T> List<T> nonEmptySubset(List<T> columns) {
		int nr = 1 + ThreadLocalRandom.current().nextInt(columns.size());
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
		int nr = ThreadLocalRandom.current().nextInt(columns.size() + 1);
		return extractNrRandomColumns(columns, nr);
	}

	public static <T> List<T> subset(int nr, @SuppressWarnings("unchecked") T... values) {
		List<T> list = new ArrayList<>();
		for (T val : values) {
			list.add(val);
		}
		return extractNrRandomColumns(list, nr);
	}

	public static <T> List<T> subset(@SuppressWarnings("unchecked") T... values) {
		List<T> list = new ArrayList<>();
		for (T val : values) {
			list.add(val);
		}
		return subset(list);
	}

	private static <T> List<T> extractNrRandomColumns(List<T> columns, int nr) {
		assert nr >= 0;
		List<T> selectedColumns = new ArrayList<>();
		List<T> remainingColumns = new ArrayList<>(columns);
		for (int i = 0; i < nr; i++) {
			selectedColumns.add(remainingColumns.remove(ThreadLocalRandom.current().nextInt(remainingColumns.size())));
		}
		return selectedColumns;
	}

	public static int smallNumber() {
		// no need to cache for small numbers
		int val = (int) (Math.abs(ThreadLocalRandom.current().nextGaussian()) * 2);
		return val;
	}

	public static boolean getBoolean() {
		return ThreadLocalRandom.current().nextBoolean();
	}

	public long getInteger() {
		if (smallBiasProbability()) {
			return Randomly.fromOptions(-1l, Long.MAX_VALUE, Long.MIN_VALUE, 1l, 0l);
		} else {
			if (cacheProbability()) {
				Long l = getFromLongCache();
				if (l != null) {
					return l;
				}
			}
			long nextLong = ThreadLocalRandom.current().nextInt();
			addToCache(nextLong);
			return nextLong;
		}
	}

	public String getString() {
		if (smallBiasProbability()) {
			return Randomly.fromOptions("TRUE", "FALSE", "0.0", "-0.0", "1e500", "-1e500");
		}
		if (cacheProbability()) {
			String s = getFromStringCache();
			if (s != null) {
				return s;
			}
		}

		int n = alphabet.length();

		StringBuilder sb = new StringBuilder();

		int chars;
		if (Randomly.getBoolean()) {
			chars = Randomly.smallNumber();
		} else {
			chars = getInteger(0, 30);
		}
		for (int i = 0; i < chars; i++) {
			sb.append(alphabet.charAt(ThreadLocalRandom.current().nextInt(n)));
		}
		while (Randomly.getBooleanWithSmallProbability()) {
			String[][] pairs = { { "{", "}" }, { "[", "]" }, { "(", ")" } };
			int idx = (int) Randomly.getNotCachedInteger(0, pairs.length);
			int left = (int) Randomly.getNotCachedInteger(0, sb.length() + 1);
			sb.insert(left, pairs[idx][0]);
			int right = (int) Randomly.getNotCachedInteger(left + 1, sb.length() + 1);
			sb.insert(right, pairs[idx][1]);
		}
		if (provider != null) {
			while (Randomly.getBooleanWithSmallProbability()) {
				if (sb.length() == 0) {
					sb.append(provider.get());
				} else {
					sb.insert((int) Randomly.getNotCachedInteger(0, sb.length()), provider.get());
				}
			}
		}

		String s = sb.toString();

		addToCache(s);
		return s;
	}

	public byte[] getBytes() {
		if (cacheProbability()) {
			byte[] val = getFromBytesCache();
			if (val != null) {
				addToCache(val);
				return val;
			}
		}
		int size = Randomly.smallNumber();
		byte[] arr = new byte[size];
		ThreadLocalRandom.current().nextBytes(arr);
		return arr;
	}

	public long getNonZeroInteger() {
		long value;
		if (smallBiasProbability()) {
			return Randomly.fromOptions(-1l, Long.MAX_VALUE, Long.MIN_VALUE, 1l);
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
		assert value != 0;
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
			value = Randomly.fromOptions(0l, Long.MAX_VALUE, 1l);
		} else {
			value = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
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
		double value = ThreadLocalRandom.current().nextDouble();
		addToCache(value);
		return value;
	}

	private static boolean smallBiasProbability() {
		return ThreadLocalRandom.current().nextInt(100) == 1;
	}

	public static boolean getBooleanWithSmallProbability() {
		return smallBiasProbability();
	}

	public int getInteger(int left, int right) {
		if (left == right) {
			return left;
		}
		return ThreadLocalRandom.current().nextInt(left, right);
	}

	public long getLong(long left, long right) {
		if (left == right) {
			return left;
		}
		return ThreadLocalRandom.current().nextLong(left, right);
	}

	public BigDecimal getRandomBigDecimal() {
		return new BigDecimal(ThreadLocalRandom.current().nextDouble());
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
		return ThreadLocalRandom.current().nextLong();
	}

	public static long getPositiveNonCachedInteger() {
		return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
	}

	public static long getPositiveOrZeroNonCachedInteger() {
		return ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
	}

	public static long getNotCachedInteger(int lower, int upper) {
		return ThreadLocalRandom.current().nextLong(lower, upper);
	}

	public Randomly(Supplier<String> provider) {
		this.provider = provider;
	}
	
	public Randomly() {
	}

}
