package lama;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public final class Randomly {

	// CACHING

	private static boolean USE_CACHING = true;
	private static final int CACHE_SIZE = 100;

	private final List<Long> cachedLongs = new ArrayList<>();
	private final List<String> cachedStrings = new ArrayList<>();
	private final List<Double> cachedDoubles = new ArrayList<>();

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
		} else if (!cachedStrings.isEmpty()) {
			return Randomly.fromList(cachedStrings);
		} else {
			return null;
		}
	}

	private static boolean cacheProbability() {
		return USE_CACHING && ThreadLocalRandom.current().nextInt(5) == 1;
	}

	// CACHING END

	public Randomly() {
	}

	public static <T> T fromList(List<T> list) {
		return list.get(ThreadLocalRandom.current().nextInt(list.size()));
	}

	@SafeVarargs
	public static <T> T fromOptions(T... options) {
		return options[ThreadLocalRandom.current().nextInt(options.length)];
	}

	public static <T> List<T> nonEmptySubset(List<T> columns) {
		int nr = 1 + ThreadLocalRandom.current().nextInt(columns.size());
		return extractNrRandomColumns(columns, nr);
	}

	public static <T> List<T> subset(List<T> columns) {
		int nr = ThreadLocalRandom.current().nextInt(columns.size() + 1);
		return extractNrRandomColumns(columns, nr);
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

	public long greaterOrEqualInt(long intValue) {
		return greaterOrEqual(intValue);
	}

	private long greaterOrEqual(long intValue) {
		if (intValue == Long.MAX_VALUE) {
			return Long.MAX_VALUE;
		}
		if (cacheProbability()) {
			Long l = getFromLongCache();
			if (l != null && l >= intValue) {
				return l;
			}
		}
		long result = ThreadLocalRandom.current().nextLong(intValue, Long.MAX_VALUE);
		addToCache(result);
		assert result >= intValue && result <= Long.MAX_VALUE : intValue + " " + result;
		return result;
	}

	public long greaterInt(long intValue) {
		if (intValue == Long.MAX_VALUE) {
			throw new IllegalArgumentException();
		}
		return greaterOrEqual(intValue + 1);
	}

	public long smallerOrEqualInt(long intValue) {
		long smallerOrEqualInt = smallerOrEqual(intValue);
		assert smallerOrEqualInt <= intValue;
		return smallerOrEqualInt;
	}

	private long smallerOrEqual(long intValue) {
		if (intValue == Long.MIN_VALUE) {
			return Long.MIN_VALUE;
		}
		if (cacheProbability()) {
			Long l = getFromLongCache();
			if (l != null && l <= intValue) {
				return l;
			}
		}
		long lessOrEqual = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, intValue);
		addToCache(lessOrEqual);
		return lessOrEqual;
	}

	public long smallerInt(long intValue) {
		if (intValue == Long.MIN_VALUE) {
			throw new IllegalArgumentException();
		}
		long smallerInt = smallerOrEqual(intValue - 1);
		assert smallerInt < intValue;
		return smallerInt;
	}

	public double smallerDouble(double value) {
		if (value == Double.NEGATIVE_INFINITY) {
			throw new IllegalArgumentException();
		} else if (value == -Double.MAX_VALUE) {
			return Double.NEGATIVE_INFINITY;
		} else {
			if (cacheProbability()) {
				Double d = getFromDoubleCache();
				if (d != null && d < value) {
					return d;
				}
			}
			double d = ThreadLocalRandom.current().nextDouble(-Double.MAX_VALUE, value);
			addToCache(d);
			return d;
		}
	}

	public static int smallNumber() {
		// no need to cache for small numbers
		return ThreadLocalRandom.current().nextInt(5);
	}

	public static boolean getBoolean() {
		return ThreadLocalRandom.current().nextBoolean();
	}

	public long notEqualInt(long intValue) {
		int randomInt;
		do {
			if (cacheProbability()) {
				Long l = getFromLongCache();
				if (l != null && intValue != l) {
					return l;
				}
			}
			randomInt = ThreadLocalRandom.current().nextInt();
		} while (randomInt == intValue);
		assert intValue != randomInt;
		addToCache(randomInt);
		return randomInt;
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

		String alphabet = new String("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#<>/.öä~-+' ");
		int n = alphabet.length();

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < Randomly.smallNumber(); i++) {
			sb.append(alphabet.charAt(ThreadLocalRandom.current().nextInt(n)));
		}

		String s = sb.toString();
		addToCache(s);
		return s;
	}

	public static void getBytes(byte[] bytes) {
		// TODO bias for bytes
		ThreadLocalRandom.current().nextBytes(bytes);
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

	public double getNonZeroReal() {
		double value;
		if (smallBiasProbability()) {
			return Randomly.fromOptions(1.0, -1.0);
		}
		if (cacheProbability()) {
			Double d = getFromDoubleCache();
			if (d != null && d != 0) {
				return d;
			}
		}

		do {
			value = getDouble();
		} while (value == 0.0);
		assert value != 0.0;
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

	public static String greaterOrEqualString(String value) {
		return value; // TODO
	}

	public double greaterOrEqualDouble(double asDouble) {
		if (asDouble == Double.POSITIVE_INFINITY) {
			return asDouble;
		} else if (asDouble == Double.MAX_VALUE) {
			return Randomly.fromOptions(Double.POSITIVE_INFINITY, Double.MAX_VALUE);
		} else if (cacheProbability()) {
			Double d = getFromDoubleCache();
			if (d != null && d >= asDouble) {
				return d;
			}
		}
		double val = ThreadLocalRandom.current().nextDouble(asDouble, Double.MAX_VALUE);
		addToCache(val);
		return val;
	}

	public double smallerOrEqualDouble(double value) {
		if (value == Double.NEGATIVE_INFINITY) {
			return value;
		} else if (value == -Double.MAX_VALUE) {
			return Randomly.fromOptions(-Double.MAX_VALUE, Double.NEGATIVE_INFINITY);
		} else if (cacheProbability()) {
			Double d = getFromDoubleCache();
			if (d != null && d <= value) {
				return d;
			}
		}
		double rand = ThreadLocalRandom.current().nextDouble(-Double.MAX_VALUE, value);
		addToCache(rand);
		return rand;
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
		return ThreadLocalRandom.current().nextDouble();
	}

	private static boolean smallBiasProbability() {
		return ThreadLocalRandom.current().nextInt(1000) == 1;
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

	public String getNonZeroString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getNonZeroInteger());
		sb.append(getString());
		return sb.toString();
	}

	public double greaterDouble(double value) {
		if (value == Double.POSITIVE_INFINITY) {
			throw new IllegalArgumentException();
		} else if (value == Double.MAX_VALUE) {
			return Double.POSITIVE_INFINITY;
		} else if (cacheProbability()) {
			Double d = getFromDoubleCache();
			if (d != null && d > value) {
				return d;
			}
		}
		return ThreadLocalRandom.current().nextDouble(value + 1, Double.MAX_VALUE);
	}

}
