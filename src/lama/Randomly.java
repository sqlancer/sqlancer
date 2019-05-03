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
		return greaterOrEqual(intValue);
	}

	private static long greaterOrEqual(long intValue) {
		long result = ThreadLocalRandom.current().nextLong(intValue, Long.MAX_VALUE);
		assert result >= intValue && result <= Long.MAX_VALUE : intValue + " " + result;
		return result;
	}

	public static long greaterInt(long intValue) {
		if (intValue == Long.MAX_VALUE) {
			throw new IllegalArgumentException();
		}
		return greaterOrEqual(intValue + 1);
	}

	public static long smallerOrEqualInt(long intValue) {
		long smallerOrEqualInt = smallerOrEqual(intValue);
		assert smallerOrEqualInt <= intValue;
		return smallerOrEqualInt;
	}

	private static long smallerOrEqual(long intValue) {
		long lessOrEqual = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, intValue);
		return lessOrEqual;
	}

	public static long smallerInt(long intValue) {
		if (intValue == Long.MIN_VALUE) {
			throw new IllegalArgumentException();
		}
		long smallerInt = smallerOrEqual(intValue - 1);
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
		assert intValue != randomInt;
		return randomInt;
	}

	public static long getInteger() {
		if (smallBiasProbability()) {
			return Randomly.fromOptions(-1l, Long.MAX_VALUE, Long.MIN_VALUE, 1l, 0l);
		}
		return ThreadLocalRandom.current().nextInt();
	}

	public static String getString() {
		if (smallBiasProbability()) {
			return Randomly.fromOptions("cafe", "asdf", "test", "TRUE", "FALSE", "0.0");
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

	public static long getNonZeroInteger() {
		long value;
		if (smallBiasProbability()) {
			return Randomly.fromOptions(-1l, Long.MAX_VALUE, Long.MIN_VALUE, 1l);
		}
		do {
			value = Randomly.getInteger();
		} while (value == 0);
		assert value != 0;
		return value;
	}

	public static long getPositiveInteger() {
		long value;
		if (smallBiasProbability()) {
			value = Randomly.fromOptions(0l, Long.MAX_VALUE, 1l);
		} else {
			value = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
		}
		assert value >= 0;
		return value;
	}

	public static String greaterOrEqualString(String value) {
		return value; // TODO
	}

	public static double greaterOrEqualDouble(double asDouble) {
		return asDouble; // TODO
	}

	public static double getDouble() {
		if (smallBiasProbability()) {
			return Randomly.fromOptions(3.3, 5.0, 0.0, -8.0);
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
