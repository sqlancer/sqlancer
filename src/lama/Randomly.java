package lama;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public final class Randomly {

	private Randomly() {
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

	public static long greaterOrEqualInt(long intValue) {
		// TODO adopt to long
		return greaterOrEqualIntDependingOnRange(intValue, 0);
	}

	private static long greaterOrEqualIntDependingOnRange(long intValue, int startRange) {
		// TODO change to long
		long range = (long) Integer.MAX_VALUE - intValue;
		long result = ThreadLocalRandom.current().nextLong(startRange, range) + intValue;
		assert result >= intValue && result <= Integer.MAX_VALUE : intValue + " " + result;
		long greaterOrEqual = (long) result;
		return greaterOrEqual;
	}

	public static long greaterInt(long intValue) {
		// TODO adopt to long
		if (intValue == Integer.MAX_VALUE) {
			throw new IllegalArgumentException();
		}
		return greaterOrEqualIntDependingOnRange(intValue, 1);
	}

	public static long smallerOrEqualInt(long intValue) {
		// TODO bound must be greater than origin
		long smallerOrEqualInt = smallerOrEqualDependingOnStartRange(intValue, 0);
		assert smallerOrEqualInt <= intValue;
		return smallerOrEqualInt;
	}

	private static long smallerOrEqualDependingOnStartRange(long intValue, int startRange) {
		// TODO adopt for longs
		long range = (long) Integer.MAX_VALUE - intValue + 1;
		// TODO bound must be greater than origin
		try {
			long lessOrEqual = intValue - ThreadLocalRandom.current().nextLong(startRange, range);
			assert lessOrEqual >= Integer.MIN_VALUE && lessOrEqual <= intValue : intValue + " " + lessOrEqual;
			return (int) lessOrEqual;
		} catch (IllegalArgumentException e) {
			throw new AssertionError(startRange + " " + range + " " + intValue,  e);
		}
	}

	public static long smallerInt(long intValue) {
		// TODO adopt for longs
		if (intValue == Integer.MIN_VALUE) {
			throw new IllegalArgumentException();
		}
		long smallerInt = smallerOrEqualDependingOnStartRange(intValue, 1);
		assert smallerInt < intValue;
		return smallerInt;
	}

	public static int smallNumber() {
		return ThreadLocalRandom.current().nextInt(10);
	}

	public static boolean getBoolean() {
		return ThreadLocalRandom.current().nextBoolean();
	}

	public static long notEqualInt(long intValue) {
		int randomInt;
		do {
			randomInt = ThreadLocalRandom.current().nextInt();
		} while (randomInt == intValue);
		return randomInt;
	}

	public static int getInteger() {
		return ThreadLocalRandom.current().nextInt();
	}

	public static String getString() {
		if (smallBiasProbability()) {
			return Randomly.fromOptions("cafe", "asdf", "test", "TRUE", "FALSE");
		}

		String alphabet = new String("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#<>/.öä~-+' ");
		int n = alphabet.length();

		StringBuilder sb = new StringBuilder();
//		Random r = new Random();

		for (int i = 0; i < Randomly.smallNumber(); i++) {
			sb.append(alphabet.charAt(ThreadLocalRandom.current().nextInt(n)));
		}

		return sb.toString();
//		byte[] bytes = new byte[Randomly.smallNumber()];
//		ThreadLocalRandom.current().nextBytes(bytes);
//		return new String(bytes, Charset.forName("UTF-8")).replace("\"", "\"\"");
	}

	public static void getBytes(byte[] bytes) {
		ThreadLocalRandom.current().nextBytes(bytes);
	}

	public static int getNonZeroInteger() {
		int value;
		if (smallBiasProbability()) {
			return Randomly.fromOptions(-1, Integer.MAX_VALUE, Integer.MIN_VALUE, 1);
		}
		do {
			value = Randomly.getInteger();
		} while (value == 0);
		assert value != 0;
		return value;
	}

	public static int getPositiveInteger() {
		if (smallBiasProbability()) {
			return Randomly.fromOptions(0, Integer.MAX_VALUE, Integer.MIN_VALUE, -1);
		} else {
			return ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
		}
	}

	public static String greaterOrEqualString(String value) {
		return value; // TODO
	}

	public static double greaterOrEqualDouble(double asDouble) {
		return asDouble; // TODO
	}

	public static double getDouble() {
		if (smallBiasProbability()) {
			return Randomly.fromOptions(3.3, 5.0, -8.0);
		} else {
			return ThreadLocalRandom.current().nextDouble();
		}
	}

	private static boolean smallBiasProbability() {
		return ThreadLocalRandom.current().nextInt(1000) == 1;
	}

	public static boolean getBooleanWithSmallProbability() {
		return smallBiasProbability();
	}

}
