package lama.mysql.ast;

import lama.IgnoreMeException;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLDataType;
import lama.mysql.ast.MySQLCastOperation.CastType;

public abstract class MySQLConstant extends MySQLExpression {

	public boolean isInt() {
		return false;
	}

	public boolean isNull() {
		return false;
	}

	public static class MySQLTextConstant extends MySQLConstant {

		private final String value;
		private boolean singleQuotes;

		public MySQLTextConstant(String value) {
			this.value = value;
			singleQuotes = Randomly.getBoolean();
		}

		@Override
		public boolean asBooleanNotNull() {
			// TODO implement as cast
			for (int i = value.length(); i >= 0; i--) {
				try {
					String substring = value.substring(0, i);
					Double val = Double.valueOf(substring);
					return val != 0;
				} catch (NumberFormatException e) {
					// ignore
				}
			}
			return false;
//			return castAs(CastType.SIGNED).getInt() != 0;
		}

		@Override
		public String getTextRepresentation() {
			StringBuilder sb = new StringBuilder();
			String quotes = singleQuotes ? "'" : "\"";
			sb.append(quotes);
			String text = value.replace(quotes, quotes + quotes);
			sb.append(text);
			sb.append(quotes);
			return sb.toString();
		}

		@Override
		protected MySQLConstant isEquals(MySQLConstant rightVal) {
			if (rightVal.isNull()) {
				return MySQLConstant.createNullConstant();
			} else if (rightVal.isInt()) {
				if (asBooleanNotNull()) {
					// TODO support SELECT .123 = '.123'; by converting to floating point
					throw new IgnoreMeException();
				}
				return castAs(CastType.SIGNED).isEquals(rightVal);
			} else if (rightVal.isString()) {
				return MySQLConstant.createBoolean(value.equalsIgnoreCase(rightVal.getString()));
			} else {
				throw new AssertionError(rightVal);
			}
		}

		@Override
		public String getString() {
			return value;
		}

		@Override
		public boolean isString() {
			return true;
		}

		@Override
		protected MySQLConstant castAs(CastType type) {
			String value = this.value;
			while (value.startsWith(" ") || value.startsWith("\t")) {
				value = value.substring(1);
			}
			for (int i = value.length(); i >= 0; i--) {
				try {
					String substring = value.substring(0, i);
					long val = Long.valueOf(substring);
					return MySQLConstant.createIntConstant(val);
				} catch (NumberFormatException e) {
					// ignore
				}
			}
			return MySQLConstant.createIntConstant(0);
		}

		@Override
		public String castAsString() {
			return value;
		}

		@Override
		public MySQLDataType getType() {
			return MySQLDataType.VARCHAR;
		}

		@Override
		protected MySQLConstant isLessThan(MySQLConstant rightVal) {
			if (rightVal.isNull()) {
				return MySQLConstant.createNullConstant();
			} else if (rightVal.isInt()) {
				if (asBooleanNotNull()) {
					// TODO uspport floating point
					throw new IgnoreMeException();
				}
				return castAs(CastType.SIGNED).isLessThan(rightVal);
			} else if (rightVal.isString()) {
				// unexpected result for '-' < "!";
//				return MySQLConstant.createBoolean(value.compareToIgnoreCase(rightVal.getString()) < 0);
				throw new IgnoreMeException();
			} else {
				throw new AssertionError(rightVal);
			}
		}

	}

	public static class MySQLIntConstant extends MySQLConstant {

		private final long value;
		private final String stringRepresentation;

		public MySQLIntConstant(long value) {
			this.value = value;
			if (value == 0 && Randomly.getBoolean()) {
				stringRepresentation = "FALSE";
			} else if (value == 1 && Randomly.getBoolean()) {
				stringRepresentation = "TRUE";
			} else {
				stringRepresentation = String.valueOf(value);
			}
		}

		public MySQLIntConstant(long value, String stringRepresentation) {
			this.value = value;
			this.stringRepresentation = stringRepresentation;
		}

		@Override
		public boolean isInt() {
			return true;
		}

		@Override
		public long getInt() {
			return value;
		}

		@Override
		public boolean asBooleanNotNull() {
			return value != 0;
		}

		@Override
		public String getTextRepresentation() {
			return stringRepresentation;
		}

		@Override
		protected MySQLConstant isEquals(MySQLConstant rightVal) {
			if (rightVal.isInt()) {
				long intVal = rightVal.getInt();
				return MySQLConstant.createBoolean(value == intVal);
			} else if (rightVal.isNull()) {
				return MySQLConstant.createNullConstant();
			} else if (rightVal.isString()) {
				if (rightVal.asBooleanNotNull()) {
					// TODO support SELECT .123 = '.123'; by converting to floating point
					throw new IgnoreMeException();
				}
				return isEquals(rightVal.castAs(CastType.SIGNED));
			} else {
				throw new AssertionError(rightVal);
			}
		}

		@Override
		protected MySQLConstant castAs(CastType type) {
			if (type == CastType.SIGNED) {
				return this;
			} else {
				throw new AssertionError();
			}
		}

		@Override
		public String castAsString() {
			return String.valueOf(value);
		}

		@Override
		public MySQLDataType getType() {
			return MySQLDataType.INT;
		}

		@Override
		protected MySQLConstant isLessThan(MySQLConstant rightVal) {
			if (rightVal.isInt()) {
				long intVal = rightVal.getInt();
				return MySQLConstant.createBoolean(value < intVal);
			} else if (rightVal.isNull()) {
				return MySQLConstant.createNullConstant();
			} else if (rightVal.isString()) {
				if (rightVal.asBooleanNotNull()) {
					// TODO support float
					throw new IgnoreMeException();
				}
				return isLessThan(rightVal.castAs(CastType.SIGNED));
			} else {
				throw new AssertionError(rightVal);
			}
		}

	}

	public static class MySQLNullConstant extends MySQLConstant {

		@Override
		public boolean isNull() {
			return true;
		}

		@Override
		public boolean asBooleanNotNull() {
			throw new UnsupportedOperationException(this.toString());
		}

		@Override
		public String getTextRepresentation() {
			return "NULL";
		}

		@Override
		protected MySQLConstant isEquals(MySQLConstant rightVal) {
			return MySQLNullConstant.createNullConstant();
		}

		@Override
		protected MySQLConstant castAs(CastType type) {
			return this;
		}

		@Override
		public String castAsString() {
			return "NULL";
		}

		@Override
		public MySQLDataType getType() {
			return null;
		}

		@Override
		protected MySQLConstant isLessThan(MySQLConstant rightVal) {
			return this;
		}

	}

	public long getInt() {
		throw new UnsupportedOperationException();
	}

	public String getString() {
		throw new UnsupportedOperationException();
	}

	public boolean isString() {
		return false;
	}

	public static MySQLConstant createNullConstant() {
		return new MySQLNullConstant();
	}

	public static MySQLConstant createIntConstant(long value) {
		return new MySQLIntConstant(value);
	}

	public static MySQLConstant createIntConstantNotAsBoolean(long value) {
		return new MySQLIntConstant(value, String.valueOf(value));
	}

	@Override
	public MySQLConstant getExpectedValue() {
		return this;
	}

	public abstract boolean asBooleanNotNull();

	public abstract String getTextRepresentation();

	public static MySQLConstant createFalse() {
		return MySQLConstant.createIntConstant(0);
	}

	public static MySQLConstant createBoolean(boolean isTrue) {
		return MySQLConstant.createIntConstant(isTrue ? 1 : 0);
	}

	public static MySQLConstant createTrue() {
		return MySQLConstant.createIntConstant(1);
	}

	@Override
	public String toString() {
		return getTextRepresentation();
	}

	protected abstract MySQLConstant isEquals(MySQLConstant rightVal);

	public MySQLConstant isEqualsNullSafe(MySQLConstant rightVal) {
		if (isNull()) {
			return MySQLConstant.createBoolean(rightVal.isNull());
		} else if (rightVal.isNull()) {
			return MySQLConstant.createFalse();
		} else {
			return isEquals(rightVal);
		}
	}

	protected abstract MySQLConstant castAs(CastType type);
	
	public abstract String castAsString();

	public static MySQLConstant createStringConstant(String string) {
		return new MySQLTextConstant(string);
	}

	public abstract MySQLDataType getType();

	protected abstract MySQLConstant isLessThan(MySQLConstant rightVal);

}
