package lama.cockroachdb.ast;

public class CockroachDBConstant extends CockroachDBExpression {

	public static class CockroachDBNullConstant extends CockroachDBConstant {

		@Override
		public String toString() {
			return "NULL";
		}

	}

	public static class CockroachDBIntConstant extends CockroachDBConstant {

		private final long value;

		public CockroachDBIntConstant(long value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return String.valueOf(value);
		}

		public long getValue() {
			return value;
		}

	}

	public static class CockroachDBDoubleConstant extends CockroachDBConstant {
		
		private final double value;

		public CockroachDBDoubleConstant(double value) {
			this.value = value;
		}

		public double getValue() {
			return value;
		}
		
		@Override
		public String toString() {
			if (value == Double.POSITIVE_INFINITY) {
				return "FLOAT '+Inf'"; 
			} else if (value == Double.NEGATIVE_INFINITY) {
				return "FLOAT '-Inf'";
			}
			return String.valueOf(value);
		}
		
	}

	public static class CockroachDBTextConstant extends CockroachDBConstant {

		private final String value;

		public CockroachDBTextConstant(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		@Override
		public String toString() {
			return "'" + value.replace("'", "''") + "'";
		}

	}
	
	public static class CockroachDBBitConstant extends CockroachDBConstant {

		private final String value;

		public CockroachDBBitConstant(long value) {
			this.value = Long.toBinaryString(value);
		}

		public String getValue() {
			return value;
		}

		@Override
		public String toString() {
			return "B'" + value + "'";
		}

	}
	
	public static class CockroachDBBooleanConstant extends CockroachDBConstant {
		
		private final boolean value;
		
		public CockroachDBBooleanConstant(boolean value) {
			this.value = value;
		}
		
		public boolean getValue() {
			return value;
		}

		@Override
		public String toString() {
			return String.valueOf(value);
		}
		
	}
	
	public static CockroachDBTextConstant createStringConstant(String text) {
		return new CockroachDBTextConstant(text);
	}
	
	public static CockroachDBDoubleConstant createFloatConstant(double val) {
		return new CockroachDBDoubleConstant(val);
	}
	
	public static CockroachDBIntConstant createIntConstant(long val) {
		return new CockroachDBIntConstant(val);
	}
	
	public static CockroachDBNullConstant createNullConstant() {
		return new CockroachDBNullConstant();
	}

	public static CockroachDBConstant createBooleanConstant(boolean val) {
		return new CockroachDBBooleanConstant(val);
	}

	public static CockroachDBExpression createBitConstant(long integer) {
		return new CockroachDBBitConstant(integer);
	}

}
