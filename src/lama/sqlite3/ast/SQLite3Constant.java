package lama.sqlite3.ast;

import java.awt.event.KeyEvent;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation.BinaryOperator;
import lama.sqlite3.gen.QueryGenerator;
import lama.sqlite3.gen.SQLite3Cast;
import lama.sqlite3.schema.SQLite3DataType;

public abstract class SQLite3Constant extends SQLite3Expression {

	public static class SQLite3NullConstant extends SQLite3Constant {

		@Override
		public boolean isNull() {
			return true;
		}

		@Override
		public Object getValue() {
			return null;
		}

		@Override
		public SQLite3DataType getDataType() {
			return SQLite3DataType.NULL;
		}

		@Override
		public List<BinaryOperator> compare(SQLite3Constant cons, boolean shouldBeTrue) {

			List<BinaryOperator> values;
			if (cons instanceof SQLite3NullConstant) {
				values = Arrays.asList(BinaryOperator.IS);
			} else {
				values = Arrays.asList(BinaryOperator.IS_NOT);
			}
			if (shouldBeTrue) {
				return values;
			} else {
				return getReversed(values);
			}
		}

		@Override
		public SQLite3Constant applyNumericAffinity() {
			return this;
		}

		@Override
		public SQLite3Constant applyTextAffinity() {
			return this;
		}

		@Override
		String getStringRepresentation() {
			return "NULL";
		}

	}

	public static class SQLite3IntConstant extends SQLite3Constant {

		private final long value;

		public SQLite3IntConstant(long value) {
			this.value = value;
		}

		@Override
		public boolean isNull() {
			return false;
		}

		@Override
		public long asInt() {
			return value;
		}

		@Override
		public Object getValue() {
			return value;
		}

		@Override
		public SQLite3DataType getDataType() {
			return SQLite3DataType.INT;
		}

		@Override
		public List<BinaryOperator> compare(SQLite3Constant cons, boolean shouldBeTrue) {
			List<BinaryOperator> values;
			if (cons instanceof SQLite3RealConstant) {
				double otherColumnValue = cons.asDouble();
				if (value > otherColumnValue) {
					values = greaterThanList();
				} else if (value < otherColumnValue) {
					values = smallerThanList();
				} else {
					values = equalsList(false);
				}
			} else if (cons instanceof SQLite3NullConstant) {
				// SELECT 0 IS NULL; -- 0
				// SELECT 0 IS NOT NULL; -- 1
				// SELECT 0 > NULL; -- NULL
				values = Arrays.asList(BinaryOperator.IS_NOT);
			} else if (cons instanceof SQLite3IntConstant) {
				long otherColumnValue = cons.asInt();
				if (value > otherColumnValue) {
					values = greaterThanList();
				} else if (value < otherColumnValue) {
					values = smallerThanList();
				} else {
					values = equalsList(true);
				}
			} else if (cons instanceof SQLite3TextConstant) {
				// SELECT 3.0 > "50"; -- 0
				// SELECT 3.0 > "2.0"; -- 0
				// SELECT 3.0 > ""; -- 0
				// SELECT 3.0 <= ""; --1
				// SELECT 3.0 LIKE "3.0"; -- 1
				// SELECT 3.0 GLOB "3.0"; -- 1
				// SELECT 3.0 != "3.0"; -- 1
				// SELECT 3.0 IS NOT "3.0"; -- 1
				values = smallerThanList();
//				if (String.valueOf(value).contentEquals(cons.asString())) {
//					values.addAll(Arrays.asList(BinaryOperator.LIKE, BinaryOperator.GLOB));
//				}
				return Collections.emptyList();

			} else {
				assert cons instanceof SQLite3BinaryConstant;
				values = smallerThanList();
			}
			if (shouldBeTrue) {
				return values;
			} else {
				return getReversed(values);
			}
		}

		@Override
		public SQLite3Constant applyNumericAffinity() {
			return this;
		}

		@Override
		public SQLite3Constant applyTextAffinity() {
			return SQLite3Constant.createTextConstant(String.valueOf(value));
		}

		@Override
		String getStringRepresentation() {
			return String.valueOf(value);
		}
	}

	public static class SQLite3RealConstant extends SQLite3Constant {

		private final double value;

		public SQLite3RealConstant(double value) {
			this.value = value;
		}

		@Override
		public boolean isNull() {
			return false;
		}

		@Override
		public double asDouble() {
			return value;
		}

		@Override
		public Object getValue() {
			return value;
		}

		@Override
		public SQLite3DataType getDataType() {
			return SQLite3DataType.REAL;
		}

		@Override
		public List<BinaryOperator> compare(SQLite3Constant cons, boolean shouldBeTrue) {
			List<BinaryOperator> values;
			if (cons instanceof SQLite3RealConstant) {
				// When an INTEGER or REAL is compared to another INTEGER or REAL, a numerical
				// comparison is performed.
				double otherColumnValue = cons.asDouble();
				if (value > otherColumnValue) {
					values = greaterThanList();
				} else if (value < otherColumnValue) {
					values = smallerThanList();
				} else {
					values = equalsList(true);
				}
			} else if (cons instanceof SQLite3NullConstant) {
				// A value with storage class NULL is considered less than any other value
				// (including another value with storage class NULL).

				// SELECT 0 IS NULL; -- 0
				// SELECT 0 IS NOT NULL; -- 1
				// SELECT 0 > NULL; -- NULL
				values = Arrays.asList(BinaryOperator.IS_NOT);
			} else if (cons instanceof SQLite3IntConstant) {
				long otherColumnValue = cons.asInt();
				if (value > otherColumnValue) {
					values = greaterThanList();
				} else if (value < otherColumnValue) {
					values = smallerThanList();
				} else {
					values = equalsList(false);
				}
			} else if (cons instanceof SQLite3TextConstant) {
				// An INTEGER or REAL value is less than any TEXT or BLOB value.
				// SELECT 3.0 > "50"; -- 0
				// SELECT 3.0 > "2.0"; -- 0
				// SELECT 3.0 > ""; -- 0
				// SELECT 3.0 <= ""; --1
				// SELECT 3.0 LIKE "3.0"; -- 1
				// SELECT 3.0 GLOB "3.0"; -- 1
				// SELECT 3.0 != "3.0"; -- 1
				// SELECT 3.0 IS NOT "3.0"; -- 1
				values = new ArrayList<>(Arrays.asList(BinaryOperator.SMALLER, BinaryOperator.SMALLER_EQUALS,
						BinaryOperator.IS_NOT, BinaryOperator.NOT_EQUALS));
//				if (String.valueOf(value).contentEquals(cons.asString())) {
//					values.addAll(Arrays.asList(BinaryOperator.LIKE, BinaryOperator.GLOB));
//				}
				return Collections.emptyList();

			} else {
				assert cons instanceof SQLite3BinaryConstant;
				// An INTEGER or REAL value is less than any TEXT or BLOB value.
				values = new ArrayList<>(Arrays.asList(BinaryOperator.SMALLER, BinaryOperator.SMALLER_EQUALS,
						BinaryOperator.IS_NOT, BinaryOperator.NOT_EQUALS));
				// TODO what about like and glob
			}
			if (shouldBeTrue) {
				return values;
			} else {
				return getReversed(values);
			}
		}

		@Override
		public SQLite3Constant applyNumericAffinity() {
			return this;
		}

		@Override
		public SQLite3Constant applyTextAffinity() {
			return SQLite3Constant.createTextConstant(String.valueOf(value));
		}

		@Override
		String getStringRepresentation() {
			return String.valueOf(value);
		}

	}

	private static List<BinaryOperator> equalsList(boolean withGlob) {
		List<BinaryOperator> values;
		if (withGlob) {
			values = Arrays.asList(BinaryOperator.EQUALS, BinaryOperator.IS, BinaryOperator.GREATER_EQUALS,
					BinaryOperator.SMALLER_EQUALS); // , BinaryOperator.GLOB
		} else {
			values = Arrays.asList(BinaryOperator.EQUALS, BinaryOperator.IS, BinaryOperator.GREATER_EQUALS,
					BinaryOperator.SMALLER_EQUALS);
		}
		return values;
	}

	private static List<BinaryOperator> smallerThanList() {
		List<BinaryOperator> values;
		values = Arrays.asList(BinaryOperator.SMALLER, BinaryOperator.SMALLER_EQUALS, BinaryOperator.IS_NOT,
				BinaryOperator.NOT_EQUALS);
		return values;
	}

	private static List<BinaryOperator> greaterThanList() {
		List<BinaryOperator> values;
		values = Arrays.asList(BinaryOperator.GREATER, BinaryOperator.GREATER_EQUALS, BinaryOperator.IS_NOT,
				BinaryOperator.NOT_EQUALS);
		return values;
	}

	public List<BinaryOperator> getReversed(List<BinaryOperator> list) {
		List<BinaryOperator> l = new ArrayList<>(list);
		l.remove(BinaryOperator.GLOB);
		return l.stream().map(op -> op.reverse()).collect(Collectors.toList());
	}

	public static class SQLite3TextConstant extends SQLite3Constant {

		private final String text;

		public SQLite3TextConstant(String text) {
			this.text = text;
		}

		@Override
		public boolean isNull() {
			return false;
		}

		@Override
		public String asString() {
			return text;
		}

		@Override
		public Object getValue() {
			return text;
		}

		@Override
		public SQLite3DataType getDataType() {
			return SQLite3DataType.TEXT;
		}

		@Override
		public List<BinaryOperator> compare(SQLite3Constant cons, boolean shouldBeTrue) {
			List<BinaryOperator> values;
			if (cons instanceof SQLite3RealConstant || cons instanceof SQLite3IntConstant) {
				values = greaterThanList();
			} else if (cons instanceof SQLite3BinaryConstant) {
				values = smallerThanList();
			} else if (cons instanceof SQLite3NullConstant) {
				values = Arrays.asList(BinaryOperator.IS_NOT);
			} else {
				assert cons instanceof SQLite3TextConstant;
//				String otherText = cons.asString();
				// TODO implement collate
//				int compareTo = text.compareToIgnoreCase(otherText);
//				if (compareTo == 0) {
//					values = equalsList(true);
//				} else if (compareTo > 0) {
//					values = smallerThanList();
//				} else {
//					values = greaterThanList();
//				}
				return Collections.emptyList();
			}
			if (shouldBeTrue) {
				return values;
			} else {
				return getReversed(values);
			}
		}

		@Override
		public SQLite3Constant applyNumericAffinity() {
			Pattern leadingDigitPattern = Pattern.compile("[-+]?((\\d(\\d)*(\\.(\\d)*)?)|\\.(\\d)(\\d)*)([Ee][+-]?(\\d)(\\d)*)?");
			String trimmedString = text.trim();
			if (trimmedString.isEmpty()) {
				return this;
			}
			Matcher matcher = leadingDigitPattern.matcher(trimmedString);
			if (matcher.matches()) {
				SQLite3Constant castValue = SQLite3Cast.castToNumeric(this);
				return castValue;
			} else {
				return this;
			}
		}

		@Override
		public SQLite3Constant applyTextAffinity() {
			return this;
		}

		@Override
		String getStringRepresentation() {
			return text;
		}
	}

	public static class SQLite3BinaryConstant extends SQLite3Constant {

		private final byte[] bytes;

		public SQLite3BinaryConstant(byte[] bytes) {
			this.bytes = bytes;
		}

		@Override
		public boolean isNull() {
			return false;
		}

		@Override
		public SQLite3DataType getDataType() {
			return SQLite3DataType.BINARY;
		}

		@Override
		public Object getValue() {
			return bytes;
		}

		@Override
		public byte[] asBinary() {
			return bytes;
		}

		@Override
		public List<BinaryOperator> compare(SQLite3Constant cons, boolean shouldBeTrue) {
			List<BinaryOperator> values = new ArrayList<>();
			if (cons instanceof SQLite3IntConstant || cons instanceof SQLite3RealConstant
					|| cons instanceof SQLite3TextConstant) {
				// An INTEGER or REAL value is less than any TEXT or BLOB value.
				// A TEXT value is less than a BLOB value.
				values = greaterThanList();
			} else if (cons instanceof SQLite3BinaryConstant) {
				// When two BLOB values are compared, the result is determined using memcmp().
				byte[] val = cons.asBinary();
				int commonLength = Math.min(val.length, bytes.length);
				for (int i = 0; i < commonLength; i++) {
					if ((bytes[i] & 0xFF) > (val[i] & 0xFF)) {
						values = greaterThanList();
						break;
					} else if ((bytes[i] & 0xFF) < (val[i] & 0xFF)) {
						values = smallerThanList();
						break;
					}
				}
				if (values.isEmpty()) {
					if (bytes.length == val.length) {
						assert Arrays.equals(val, bytes);
						values = equalsList(false); // TODO glob?
					} else if (bytes.length > val.length) {
						values = greaterThanList();
					} else {
						values = smallerThanList();
					}
				}
			} else {
				assert cons instanceof SQLite3NullConstant;
				// A value with storage class NULL is considered less than any other value
				// (including another value with storage class NULL).
				values = Arrays.asList(BinaryOperator.IS_NOT);
			}
			if (shouldBeTrue) {
				return values;
			} else {
				return getReversed(values);
			}
		}

		@Override
		public SQLite3Constant applyNumericAffinity() {
			return this;
		}

		@Override
		public SQLite3Constant applyTextAffinity() {
			return SQLite3Constant.createTextConstant(new String(bytes));
		}

		@Override
		String getStringRepresentation() {
			String hexRepr = SQLite3Visitor.byteArrayToHex(bytes);
			return String.format("x'%s'", hexRepr);
		}

	}

	abstract String getStringRepresentation();

	public abstract boolean isNull();

	public abstract Object getValue();

	public abstract List<BinaryOperator> compare(SQLite3Constant cons, boolean shouldBeTrue);

	public long asInt() {
		throw new UnsupportedOperationException(this.getDataType().toString());
	}

	public double asDouble() {
		throw new UnsupportedOperationException(this.getDataType().toString());
	}

	public byte[] asBinary() {
		throw new UnsupportedOperationException(this.getDataType().toString());
	}

	public String asString() {
		throw new UnsupportedOperationException(this.getDataType().toString());
	}

	public abstract SQLite3DataType getDataType();

	public static SQLite3Constant createIntConstant(long val) {
		return new SQLite3IntConstant(val);
	}

	public static SQLite3Constant createBinaryConstant(byte[] val) {
		return new SQLite3BinaryConstant(val);
	}

	public static SQLite3Constant createRealConstant(double real) {
		return new SQLite3RealConstant(real);
	}

	public static SQLite3Constant createTextConstant(String text) {
		return new SQLite3TextConstant(text);
	}

	public static SQLite3Constant createNullConstant() {
		return new SQLite3NullConstant();
	}

	public static SQLite3Constant getRandomBinaryConstant() {
		int size = Randomly.smallNumber();
		byte[] arr = new byte[size];
		Randomly.getBytes(arr);
		return new SQLite3BinaryConstant(arr);
	}

	@Override
	public SQLite3Constant getExpectedValue() {
		return this;
	}

	@Override
	public String toString() {
		return String.format("(%s) %s", getDataType(), getStringRepresentation());
	}

	public abstract SQLite3Constant applyNumericAffinity();

	public abstract SQLite3Constant applyTextAffinity();

	public static SQLite3Constant createTrue() {
		return new SQLite3Constant.SQLite3IntConstant(1);
	}
	
	public static SQLite3Constant createFalse() {
		return new SQLite3Constant.SQLite3IntConstant(0);
	}
	
	public static SQLite3Constant createBoolean(boolean tr) {
		return new SQLite3Constant.SQLite3IntConstant(tr ? 1 : 0);
	}

}