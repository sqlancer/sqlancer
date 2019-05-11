package lama.sqlite3.ast;

import java.util.Arrays;

import lama.Randomly;
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

	}

	public abstract boolean isNull();

	public abstract Object getValue();

	public long asInt() {
		throw new UnsupportedOperationException();
	}

	public double asDouble() {
		throw new UnsupportedOperationException();
	}

	public byte[] asBinary() {
		throw new UnsupportedOperationException(this.getDataType().toString());
	}

	public String asString() {
		throw new UnsupportedOperationException();
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
	public String toString() {
		return String.format("(%s) %s", getDataType(),
				getValue() instanceof byte[] ? Arrays.toString((byte[]) getValue()) : getValue());
	}

}