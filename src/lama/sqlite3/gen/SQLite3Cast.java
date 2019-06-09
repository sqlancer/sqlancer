package lama.sqlite3.gen;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.regex.Pattern;

import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.schema.SQLite3DataType;

public class SQLite3Cast {

	public static Optional<Boolean> isTrue(SQLite3Constant value) {
		SQLite3Constant numericValue;
		if (value.getDataType() == SQLite3DataType.NULL) {
			return Optional.empty();
		}
		if (value.getDataType() == SQLite3DataType.TEXT || value.getDataType() == SQLite3DataType.BINARY) {
			numericValue = castToNumeric(value);
		} else {
			numericValue = value;
		}
		assert numericValue.getDataType() != SQLite3DataType.TEXT : numericValue + "should have been converted";
		switch (numericValue.getDataType()) {
		case INT:
			return Optional.of(numericValue.asInt() != 0);
		case REAL:
			return Optional.of(numericValue.asDouble() != 0 && !Double.isNaN(numericValue.asDouble()));
		default:
			throw new AssertionError(numericValue);
		}
	}

	// SELECT CAST('-1.370998801E9' AS INTEGER) == -1
	public static SQLite3Constant castToInt(SQLite3Constant cons) {
		if (cons.getDataType() == SQLite3DataType.BINARY) {
			String text = new String(cons.asBinary());
			cons = SQLite3Constant.createTextConstant(text);
		}
		switch (cons.getDataType()) {
		case NULL:
			return SQLite3Constant.createNullConstant();
		case INT:
			return cons;
		case REAL:
			return SQLite3Constant.createIntConstant((long) cons.asDouble());
		case TEXT:
			String asString = cons.asString();
			while (startsWithWhitespace(asString)) {
				asString = asString.substring(1);
			}
			if (!asString.isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(asString)) {
				return SQLite3Constant.createIntConstant(0);
			}
			for (int i = asString.length(); i >= 0; i--) {
				try {
					String substring = asString.substring(0, i);
					Pattern p = Pattern.compile("[+-]?\\d\\d*");
					if (p.matcher(substring).matches()) {
						BigDecimal bg = new BigDecimal(substring);
						long result;
						try {
							result = bg.longValueExact();
						} catch (ArithmeticException e) {
							if (substring.startsWith("-")) {
								result = Long.MIN_VALUE;
							} else {
								result = Long.MAX_VALUE;
							}
						}
						return SQLite3Constant.createIntConstant(result);
					}
				} catch (Exception e) {

				}
			}
			return SQLite3Constant.createIntConstant(0);
		default:
			throw new AssertionError();
		}

	}

	public static SQLite3Constant castToReal(SQLite3Constant cons) {
		SQLite3Constant numericValue = castToNumeric(cons);
		if (numericValue.getDataType() == SQLite3DataType.INT) {
			return SQLite3Constant.createRealConstant(numericValue.asInt());
		} else {
			return numericValue;
		}
	}

	/**
	 * Applies numeric affinity to a value.
	 */
	public static SQLite3Constant castToNumeric(SQLite3Constant value) {
		if (value.getDataType() == SQLite3DataType.BINARY) {
			String text = new String(value.asBinary());
			value = SQLite3Constant.createTextConstant(text);
		}
		switch (value.getDataType()) {
		case NULL:
			return SQLite3Constant.createNullConstant();
		case INT:
		case REAL:
			return value;
		case TEXT:
			String asString = value.asString();
			while (startsWithWhitespace(asString)) {
				asString = asString.substring(1);
			}
			if (!asString.isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(asString)) {
				return SQLite3Constant.createIntConstant(0);
			}
			if (asString.toLowerCase().startsWith("-infinity") || asString.toLowerCase().startsWith("infinity") || asString.startsWith("NaN")) {
				return SQLite3Constant.createIntConstant(0);
			}
			for (int i = asString.length(); i >= 0; i--) {
				try {
					double d = Double.valueOf(asString.substring(0, i));
					if (d == (long) d && (!asString.toUpperCase().contains("E") || d == 0)) {
						return SQLite3Constant.createIntConstant(Long.parseLong(asString.substring(0, i)));
					} else {
						return SQLite3Constant.createRealConstant(d);
					}
				} catch (Exception e) {

				}
			}
			return SQLite3Constant.createIntConstant(0);
		default:
			throw new AssertionError(value);
		}
	}

	private static boolean startsWithWhitespace(String asString) {
		if (asString.isEmpty()) {
			return false;
		}
		char c = asString.charAt(0);
		switch (c) {
		case ' ':
		case '\t':
		case 0x0b:
		case '\f':
		case '\n':
		case '\r':
			return true;
		default:
			return false;
		}
	}

	private final static byte FILE_SEPARATOR = 0x1c;
	private final static byte GROUP_SEPARATOR = 0x1d;
	private final static byte RECORD_SEPARATOR = 0x1e;
	private final static byte UNIT_SEPARATOR = 0x1f;
	private final static byte SYNCHRONOUS_IDLE = 0x16;

	private static boolean unprintAbleCharThatLetsBecomeNumberZero(String s) {
		// non-printable characters are ignored by Double.valueOf
		for (int i = 0; i < s.length(); i++) {
			char charAt = s.charAt(i);
			if (!Character.isISOControl(charAt) && !Character.isWhitespace(charAt)) {
				return false;
			}
			switch (charAt) {
			case GROUP_SEPARATOR:
			case FILE_SEPARATOR:
			case RECORD_SEPARATOR:
			case UNIT_SEPARATOR:
			case SYNCHRONOUS_IDLE:
				return true;
			}

			if (Character.isWhitespace(charAt)) {
				continue;
			} else {
				return true;
			}
		}
		return false;
	}

	public static SQLite3Constant castToText(SQLite3Constant cons) {
		if (cons.getDataType() == SQLite3DataType.TEXT) {
			return cons;
		}
		if (cons.getDataType() == SQLite3DataType.NULL) {
			return cons;
		}
//		if (cons.getDataType() == SQLite3DataType.REAL) {
//			return SQLite3Constant.createTextConstant(String.valueOf(cons.asDouble()));
//		}
		if (cons.getDataType() == SQLite3DataType.INT) {
			return SQLite3Constant.createTextConstant(String.valueOf(cons.asInt()));
		}
//		if (cons.getDataType() == SQLite3DataType.BINARY) {
//			try {
//				return SQLite3Constant.createTextConstant(new String(cons.asBinary(), "UTF-8").replaceAll("\\p{C}", ""));
//			} catch (UnsupportedEncodingException e) {
//				throw new AssertionError(e);
//			}
//		}
		return null;
//		throw new AssertionError();
	}

	public static SQLite3Constant asBoolean(SQLite3Constant val) {
		Optional<Boolean> boolVal = isTrue(val);
		if (boolVal.isPresent()) {
			return SQLite3Constant.createBoolean(boolVal.get());
		} else {
			return SQLite3Constant.createNullConstant();
		}
	}

	public static SQLite3Constant castToBlob(SQLite3Constant cons) {
		if (cons.isNull()) {
			return cons;
		} else {
			SQLite3Constant stringVal = SQLite3Cast.castToText(cons);
			if (stringVal == null) {
				return null;
			} else {
				return SQLite3Constant.createBinaryConstant(stringVal.asString().getBytes());
			}
		} 
	}

}
