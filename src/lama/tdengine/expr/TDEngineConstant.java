package lama.tdengine.expr;

import java.sql.Date;
import java.util.Optional;

import lama.IgnoreMeException;
import lama.Randomly;
import lama.tdengine.TDEngineSchema.TDEngineDataType;

public abstract class TDEngineConstant extends TDEngineExpression {
	
	public static class TDEngineNullConstant extends TDEngineConstant {
		
		@Override
		public String toString() {
			return "NULL";
		}

		@Override
		public Optional<Boolean> asBoolean() {
			return Optional.empty();
		}

		@Override
		protected TDEngineDataType getType() {
			throw new AssertionError();
		}

		@Override
		protected TDEngineConstant castAs(TDEngineDataType type) {
			return this;
		}

		@Override
		protected TDEngineConstant isEquals(TDEngineConstant expectedValue2) {
			return TDEngineConstant.createFalse();
		}
	}
	
	public static class TDEngineTimestampConstant extends TDEngineConstant {

		private long date;

		public TDEngineTimestampConstant(long val) {
			this.date = val;
		}
		
		@Override
		public String toString() {
			return new Date(date).toString();
		}

		@Override
		public Optional<Boolean> asBoolean() {
			throw new AssertionError();
		}

		@Override
		protected TDEngineDataType getType() {
			return TDEngineDataType.TIMESTAMP;
		}

		@Override
		protected TDEngineConstant castAs(TDEngineDataType type) {
			switch (type) {
			case BOOL:
				return TDEngineConstant.createBoolConstant(date != 0);
			case DOUBLE:
			case FLOAT:
				return TDEngineConstant.createDoubleConstant(date);
			case INT:
				return TDEngineConstant.createIntConstant(date);
			case TEXT:
				return TDEngineConstant.createTextConstant(toString());
			case TIMESTAMP:
				return this;
			default:
				throw new AssertionError();
			}
		}

		@Override
		protected TDEngineConstant isEquals(TDEngineConstant expectedValue2) {
			if (expectedValue2 instanceof TDEngineTimestampConstant) {
				return TDEngineConstant.createBoolConstant(date == ((TDEngineTimestampConstant) expectedValue2).date);
			} else if (expectedValue2 instanceof TDEngineNullConstant) {
				return TDEngineConstant.createFalse();
			} else {
				return TDEngineConstant.createFalse();
			}
		}
	}
	
	public static class TDEngineBooleanConstant extends TDEngineConstant {
		
		private boolean val;

		public TDEngineBooleanConstant(boolean val) {
			this.val = val;
		}
		
		@Override
		public String toString() {
			return String.valueOf(val);
		}

		@Override
		public Optional<Boolean> asBoolean() {
			return Optional.of(val);
		}

		@Override
		protected TDEngineDataType getType() {
			return TDEngineDataType.BOOL;
		}

		@Override
		protected TDEngineConstant castAs(TDEngineDataType type) {
			switch (type) {
			case BOOL:
				return this;
			case DOUBLE:
			case FLOAT:
				return TDEngineConstant.createDoubleConstant(val ? 1 : 0);
			case INT:
				return TDEngineConstant.createIntConstant(val ? 1 : 0);
			case TEXT:
				return TDEngineConstant.createTextConstant(String.valueOf(val));
			case TIMESTAMP:
				return TDEngineConstant.createTimestampConstant(val ? 1 : 0);
			default:
				throw new AssertionError();
			}
		}

		@Override
		protected TDEngineConstant isEquals(TDEngineConstant expectedValue2) {
			if (expectedValue2 instanceof TDEngineBooleanConstant) {
				return TDEngineConstant.createBoolConstant(val == ((TDEngineBooleanConstant) expectedValue2).val);
			} else if (expectedValue2 instanceof TDEngineNullConstant) {
				return TDEngineConstant.createFalse();
			} else {
				return TDEngineConstant.createFalse();
			}
		}

	}
	
	public static class TDEngineIntConstant extends TDEngineConstant {
		
		private long val;

		public TDEngineIntConstant(long val) {
			this.val = val;
		}
		
		@Override
		public String toString() {
			return String.valueOf(val);
		}

		@Override
		public Optional<Boolean> asBoolean() {
			return Optional.of(val != 0);
		}

		@Override
		protected TDEngineDataType getType() {
			return TDEngineDataType.INT;
		}

		@Override
		protected TDEngineConstant castAs(TDEngineDataType type) {
			switch (type) {
			case BOOL:
				return TDEngineConstant.createBoolConstant(val != 0);
			case DOUBLE:
			case FLOAT:
				return TDEngineConstant.createDoubleConstant(val);
			case INT:
				return this;
			case TEXT:
				return TDEngineConstant.createTextConstant(String.valueOf(val));
			case TIMESTAMP:
				return TDEngineConstant.createTimestampConstant(val);
			default:
				throw new AssertionError();
			}
		}
		
		@Override
		protected TDEngineConstant isEquals(TDEngineConstant expectedValue2) {
			if (expectedValue2 instanceof TDEngineIntConstant) {
				return TDEngineConstant.createBoolConstant(val == ((TDEngineIntConstant) expectedValue2).val);
			} else if (expectedValue2 instanceof TDEngineNullConstant) {
				return TDEngineConstant.createFalse();
			} else {
				return TDEngineConstant.createFalse();
			}
		}
	}
	
	public static class TDEngineDoubleConstant extends TDEngineConstant {
		
		private double val;

		public TDEngineDoubleConstant(double val) {
			this.val = val;
		}
		
		@Override
		public String toString() {
			return String.valueOf(val);
		}

		@Override
		public Optional<Boolean> asBoolean() {
			return Optional.of(val != 0);
		}

		@Override
		protected TDEngineDataType getType() {
			return TDEngineDataType.DOUBLE;
		}

		@Override
		protected TDEngineConstant castAs(TDEngineDataType type) {
			switch (type) {
			case BOOL:
				return TDEngineConstant.createBoolConstant(val != 0);
			case DOUBLE:
			case FLOAT:
				return this;
			case INT:
				return TDEngineConstant.createIntConstant((long) val);
			case TEXT:
				return TDEngineConstant.createTextConstant(String.valueOf(val));
			case TIMESTAMP:
				return TDEngineConstant.createTimestampConstant((long) val);
			default:
				throw new AssertionError();
			}
		}
		
		@Override
		protected TDEngineConstant isEquals(TDEngineConstant expectedValue2) {
			if (expectedValue2 instanceof TDEngineDoubleConstant) {
				return TDEngineConstant.createBoolConstant(val == ((TDEngineDoubleConstant) expectedValue2).val);
			} else if (expectedValue2 instanceof TDEngineNullConstant) {
				return TDEngineConstant.createFalse();
			} else {
				return TDEngineConstant.createFalse();
			}
		}
	}
	
	public static class TDEngineTextConstant extends TDEngineConstant {
		
		private String val;

		public TDEngineTextConstant(String text) {
			String quotes;
			if (Randomly.getBoolean()) {
				quotes = "'";
			} else {
				quotes = "\"";
			}
			this.val = String.format("%s%s%s", quotes, text.replace(quotes, "\\" + quotes), quotes);
		}
		
		@Override
		public String toString() {
			return val;
		}

		@Override
		public Optional<Boolean> asBoolean() {
			throw new AssertionError(); // TODO
		}

		@Override
		protected TDEngineDataType getType() {
			return TDEngineDataType.TEXT;
		}

		@Override
		protected TDEngineConstant castAs(TDEngineDataType type) {
			throw new IgnoreMeException(); // FIXME
		}
		
		@Override
		protected TDEngineConstant isEquals(TDEngineConstant expectedValue2) {
			if (expectedValue2 instanceof TDEngineTextConstant) {
				return TDEngineConstant.createBoolConstant(val == ((TDEngineTextConstant) expectedValue2).val);
			} else if (expectedValue2 instanceof TDEngineNullConstant) {
				return TDEngineConstant.createFalse();
			} else {
				return TDEngineConstant.createFalse();
			}
		}
	}
	
	public static TDEngineConstant createNull() {
		return new TDEngineNullConstant();
	}

	public static TDEngineConstant createFalse() {
		return TDEngineConstant.createBoolConstant(false);
	}
	
	public static TDEngineConstant createTrue() {
		return TDEngineConstant.createBoolConstant(true);
	}

	public static TDEngineConstant createIntConstant(long val) {
		return new TDEngineIntConstant(val);
	}

	public abstract Optional<Boolean> asBoolean();
	
	protected abstract TDEngineDataType getType();

	
	@Override
	public TDEngineConstant getExpectedValue() {
		return this;
	}

	protected abstract TDEngineConstant castAs(TDEngineDataType type);

	public static TDEngineExpression createRandomBoolConstant() {
		return new TDEngineBooleanConstant(Randomly.getBoolean());
	}

	public static TDEngineExpression createRandomDoubleConstant(Randomly r) {
		return new TDEngineDoubleConstant(r.getDouble());
	}

	public static TDEngineExpression createRandomIntConstant(Randomly r) {
		return new TDEngineIntConstant(r.getInteger());
	}

	public static TDEngineExpression createRandomTextConstant(Randomly r) {
		return new TDEngineTextConstant(r.getString());
	}

	public static TDEngineExpression createRandomTimestamp(Randomly r) {
		return new TDEngineTimestampConstant(r.getInteger());
	}

	public static TDEngineConstant createBoolConstant(boolean val) {
		return new TDEngineBooleanConstant(val);
	}

	public static TDEngineConstant createTextConstant(String val) {
		return new TDEngineTextConstant(val);
	}

	public static TDEngineConstant createDoubleConstant(double val) {
		return new TDEngineDoubleConstant(val);
	}

	public static TDEngineConstant createTimestampConstant(long val) {
		return new TDEngineTimestampConstant(val);

	}

	protected abstract TDEngineConstant isEquals(TDEngineConstant expectedValue2);

}
